import Foundation
import LoggerFilePersistence
import LoggerPersistence

/// Internal serial worker that drains accepted ``PersistentLogEnvelope``
/// values from an `AsyncStream` into a ``FileLogStore``.
///
/// `FileLogger.log(_:_:_:attributes:)` is synchronous, so the
/// worker enqueues envelopes via `AsyncStream.Continuation.yield`
/// (sync, thread-safe) and a single retained consumer task drains
/// them in arrival order through `FileLogStore.append(_:)`. The
/// stream uses `bufferingOldest(queueCapacity)` so the worker
/// keeps the oldest accepted envelopes moving and drops the
/// newest yields once the buffer hits capacity (drop-newest in
/// producer-side terms); rejected yields fire the
/// ``FileLoggerDiagnostic/bufferOverflow`` signal so the host can
/// observe overflow without making `Logger.log` async or
/// throwing.
///
/// Accepted FIFO order is preserved: two yields that both reach
/// `.enqueued` are drained in the order the bounded buffer
/// admitted them. Concurrent producers do not receive a stronger
/// global ordering guarantee; the bounded buffer's accepted order
/// is the contract.
///
/// ``drainBarrier()`` reads the accepted-yield count under a
/// lock and waits until the consumer task has finished processing
/// at least that many envelopes. `FileLogger.flush()` /
/// `exportLogs(to:)` use the barrier to ensure
/// `FileLogStore.flush()` and `FileLogStore.exportLogs(to:)` see
/// every accepted entry on disk.
///
/// Lifetime ownership flows `worker → task → state`: the consumer
/// task captures the bookkeeping ``FileLoggerWorkerState``
/// **strongly**, so the worker holding the task indirectly keeps
/// the state alive for the task's entire run. There is no
/// `task → worker` back-reference; cooperative teardown via
/// ``finish()`` only closes the producer side of the stream, and
/// the consumer task drains buffered envelopes and resumes any
/// outstanding barriers on its own before exiting.
final class FileLoggerWorker: @unchecked Sendable {
    private let continuation: AsyncStream<PersistentLogEnvelope>.Continuation
    private let state: FileLoggerWorkerState
    private let task: Task<Void, Never>

    init(
        store: any FileLoggerAppendStore,
        queueCapacity: Int,
        onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)?
    ) {
        let (stream, continuation) = AsyncStream<PersistentLogEnvelope>.makeStream(
            bufferingPolicy: .bufferingOldest(queueCapacity)
        )
        self.continuation = continuation
        let state = FileLoggerWorkerState(onDiagnostic: onDiagnostic)
        self.state = state

        // Strong-capture: the consumer task owns its own
        // reference to the bookkeeping state through the closure
        // capture list, so a lost or reordered `init` write order
        // cannot cause the drain loop to look up an `Optional`
        // and skip `recordDrained()`. The worker owns the task
        // (worker → task → state); there is no back-reference
        // from the task to the worker.
        task = Task.detached { [stream, state] in
            for await envelope in stream {
                // `do throws(FileLogStoreError)` pins the inner
                // throw type so the catch binding is the typed
                // `FileLogStoreError` value verbatim — no `as?`
                // cast, no untyped `any Error` fallback. If
                // `FileLogStore.append(_:)` ever switches to a
                // wider untyped throws, this `do throws(...)`
                // annotation refuses to compile at the SwiftPM
                // build step instead of silently dropping an
                // accepted envelope through a generic catch.
                do throws(FileLogStoreError) {
                    try await store.append(envelope)
                } catch {
                    state.recordAppendFailure(error)
                }
                state.recordDrained()
            }
            state.resumeAllBarriersAtStreamFinish()
        }
    }

    /// Synchronously enqueues `envelope` for delivery. Inspects
    /// the bounded buffer's yield outcome and fires the
    /// `bufferOverflow` diagnostic when the buffer was at
    /// capacity and the yield was dropped on the producer side.
    ///
    /// The yield + `acceptedCount` bookkeeping run inside a single
    /// `FileLoggerWorkerState.yieldAndCount(_:via:)` critical
    /// section so a concurrent ``drainBarrier()`` cannot observe a
    /// state where the envelope has already entered the stream's
    /// buffer but `acceptedCount` has not yet been incremented.
    /// Without that ordering, ``FileLogger/flush()`` /
    /// ``FileLogger/exportLogs(to:)`` could return before the
    /// just-accepted envelope reached `FileLogStore.append(_:)`.
    func enqueue(_ envelope: PersistentLogEnvelope) {
        let result = state.yieldAndCount(envelope, via: continuation)
        switch result {
        case .enqueued:
            break
        case .dropped:
            state.fireBufferOverflow()
        case .terminated:
            // The stream is already finished (e.g. logger is
            // tearing down). The envelope is dropped — firing
            // the overflow signal at lifecycle teardown would be
            // a false positive, so do nothing.
            break
        @unknown default:
            // Future yield outcomes default to drop semantics
            // from the host's perspective: the envelope did not
            // reach the store.
            state.fireBufferOverflow()
        }
    }

    /// Suspends until the consumer task has finished processing
    /// every envelope that was accepted into the bounded buffer
    /// at the moment this call captured `acceptedCount`. Newer
    /// yields that arrive after the snapshot do not block the
    /// barrier.
    func drainBarrier() async {
        await state.drainBarrier()
    }

    /// Cooperative teardown: closes the producer side of the
    /// stream so future ``enqueue(_:)`` calls observe
    /// `YieldResult.terminated` and drop without firing a
    /// false-positive overflow signal. Envelopes that the bounded
    /// buffer **already admitted** before `finish()` keep
    /// draining through the consumer task in arrival order; the
    /// task exits the `for await` loop only after the buffer is
    /// empty. Any outstanding ``drainBarrier()`` callers either
    /// hit their target through the normal drain path or are
    /// released by ``FileLoggerWorkerState/resumeAllBarriersAtStreamFinish()``
    /// once the loop exits.
    ///
    /// `finish()` does **not** cancel the task. Cancelling here
    /// would let the consumer abandon already-accepted envelopes
    /// before they reach `FileLogStore.append(_:)`; the
    /// caller-driven persistent lifecycle (`flush()` /
    /// `exportLogs(to:)`) relies on accepted envelopes always
    /// reaching disk.
    func finish() {
        continuation.finish()
    }

    /// Test-only: awaits the consumer task's completion.
    /// Lifecycle tests use this to assert that ``finish()``
    /// drains accepted buffered envelopes cooperatively rather
    /// than abandoning them.
    func awaitDrainCompletion() async {
        await task.value
    }
}

/// Bookkeeping state shared between the producer (`enqueue`,
/// `drainBarrier`) and the consumer task (`recordDrained` after
/// each `FileLogStore.append(_:)`). Held by the consumer task
/// through a strong capture so the task never reaches through an
/// `Optional` weak reference for drain accounting.
final class FileLoggerWorkerState: @unchecked Sendable {
    private let onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)?
    private let lock = NSLock()
    private var acceptedCount: UInt64 = 0
    private var drainedCount: UInt64 = 0
    private var barriers: [(UInt64, CheckedContinuation<Void, Never>)] = []
    private var streamFinished = false

    init(onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)?) {
        self.onDiagnostic = onDiagnostic
    }

    /// Atomically yields `envelope` to `continuation` and, if the
    /// yield was accepted, increments `acceptedCount` before
    /// releasing the lock. Returns the bounded buffer's yield
    /// result so the caller can route the `bufferOverflow`
    /// diagnostic outside the lock.
    func yieldAndCount(
        _ envelope: PersistentLogEnvelope,
        via continuation: AsyncStream<PersistentLogEnvelope>.Continuation
    ) -> AsyncStream<PersistentLogEnvelope>.Continuation.YieldResult {
        lock.lock()
        let result = continuation.yield(envelope)
        if case .enqueued = result {
            acceptedCount &+= 1
        }
        lock.unlock()
        return result
    }

    /// Fires the bounded-buffer overflow diagnostic from outside
    /// the lock so a host sink that re-enters logging on overflow
    /// cannot recursively reacquire the state's lock.
    func fireBufferOverflow() {
        onDiagnostic?(.bufferOverflow)
    }

    /// Fires the append-failure diagnostic for a typed
    /// `FileLogStoreError` the consumer task captured.
    func recordAppendFailure(_ error: FileLogStoreError) {
        onDiagnostic?(.appendFailed(error))
    }

    /// Called by the consumer task after each
    /// `FileLogStore.append(_:)` (success or typed-failure)
    /// completes. Increments `drainedCount` and resumes any
    /// outstanding barriers that have reached their target.
    func recordDrained() {
        var toResume: [CheckedContinuation<Void, Never>] = []
        lock.lock()
        drainedCount &+= 1
        var remaining: [(UInt64, CheckedContinuation<Void, Never>)] = []
        for entry in barriers {
            if drainedCount >= entry.0 {
                toResume.append(entry.1)
            } else {
                remaining.append(entry)
            }
        }
        barriers = remaining
        lock.unlock()
        for continuation in toResume {
            continuation.resume()
        }
    }

    /// Suspends until the consumer task has drained at least the
    /// envelopes that were already accepted into the bounded
    /// buffer at the moment this call captured `acceptedCount`.
    func drainBarrier() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            let target = acceptedCount
            if streamFinished || drainedCount >= target {
                lock.unlock()
                continuation.resume()
            } else {
                barriers.append((target, continuation))
                lock.unlock()
            }
        }
    }

    /// Called by the consumer task after its `for await` loop
    /// has exited (the producer side closed and the buffer
    /// drained). Releases any barriers that did not reach their
    /// target through the normal drain path.
    func resumeAllBarriersAtStreamFinish() {
        var toResume: [CheckedContinuation<Void, Never>] = []
        lock.lock()
        streamFinished = true
        toResume = barriers.map(\.1)
        barriers.removeAll()
        lock.unlock()
        for continuation in toResume {
            continuation.resume()
        }
    }
}
