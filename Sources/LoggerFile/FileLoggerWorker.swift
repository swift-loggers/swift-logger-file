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
final class FileLoggerWorker: @unchecked Sendable {
    /// Default upper bound on the number of pending envelopes the
    /// worker's bounded buffer admits. Reaching this bound starts
    /// dropping new yields on the producer side.
    static let defaultQueueCapacity = 1000

    private let continuation: AsyncStream<PersistentLogEnvelope>.Continuation
    private let task: Task<Void, Never>
    private let onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)?

    private let lock = NSLock()
    private var acceptedCount: UInt64 = 0
    private var drainedCount: UInt64 = 0
    private var barriers: [(UInt64, CheckedContinuation<Void, Never>)] = []
    private var streamFinished = false

    init(
        store: FileLogStore,
        queueCapacity: Int,
        onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)?
    ) {
        self.onDiagnostic = onDiagnostic
        let (stream, continuation) = AsyncStream<PersistentLogEnvelope>.makeStream(
            bufferingPolicy: .bufferingOldest(queueCapacity)
        )
        self.continuation = continuation

        // Capture the worker through a weak class-bound box so
        // the consumer task can call back into it after each
        // append without forcing `self` into the task body.
        let workerBox = WorkerWeakRef()
        let captured = workerBox
        let onAppendDiagnostic = onDiagnostic

        task = Task.detached { [stream, captured, onAppendDiagnostic] in
            for await envelope in stream {
                do {
                    try await store.append(envelope)
                } catch let error as FileLogStoreError {
                    onAppendDiagnostic?(.appendFailed(error))
                } catch {
                    // `FileLogStore.append(_:)` is typed-throws
                    // `FileLogStoreError`; this branch is reserved
                    // for a future seam change. The envelope is
                    // dropped silently — the logger's
                    // synchronous-infallible contract holds and
                    // later envelopes keep flowing.
                }
                captured.worker?.recordDrained()
            }
            // Stream terminated; resume any outstanding barriers
            // so `flush()` / `exportLogs(to:)` cannot hang past
            // the worker's lifetime.
            captured.worker?.resumeAllBarriersAtStreamFinish()
        }
        workerBox.worker = self
    }

    /// Synchronously enqueues `envelope` for delivery. Inspects
    /// the bounded buffer's yield outcome and fires the
    /// `bufferOverflow` diagnostic when the buffer was at
    /// capacity and the yield was dropped on the producer side.
    func enqueue(_ envelope: PersistentLogEnvelope) {
        let result = continuation.yield(envelope)
        switch result {
        case .enqueued:
            lock.lock()
            acceptedCount &+= 1
            lock.unlock()
        case .dropped:
            onDiagnostic?(.bufferOverflow)
        case .terminated:
            // The stream is already finished (e.g. logger is
            // tearing down). The envelope is dropped — firing the
            // overflow signal at lifecycle teardown would be a
            // false positive, so do nothing.
            break
        @unknown default:
            // Future yield outcomes default to drop semantics
            // from the host's perspective: the envelope did not
            // reach the store.
            onDiagnostic?(.bufferOverflow)
        }
    }

    /// Suspends until the consumer task has finished processing
    /// every envelope that was accepted into the bounded buffer
    /// at the moment this call captured `acceptedCount`. Newer
    /// yields that arrive after the snapshot do not block the
    /// barrier.
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

    /// Terminates the consumer task cooperatively; called from
    /// ``FileLogger``'s deinit-like teardown. Outstanding barriers
    /// complete immediately so a caller that was awaiting
    /// `flush()` during teardown does not hang.
    func finish() {
        continuation.finish()
        task.cancel()
    }

    private func recordDrained() {
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

    private func resumeAllBarriersAtStreamFinish() {
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

/// Weak reference box that lets the consumer task call back into
/// the worker after each append without capturing `self` strongly
/// inside the task body.
private final class WorkerWeakRef: @unchecked Sendable {
    weak var worker: FileLoggerWorker?
}
