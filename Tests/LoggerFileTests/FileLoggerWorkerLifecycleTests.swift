import Foundation
import LoggerFilePersistence
import LoggerPersistence
import Loggers
import Testing

@testable import LoggerFile

/// Coverage for ``FileLoggerWorker`` lifecycle and barrier
/// accounting that the public ``FileLogger`` contract cannot
/// exercise directly: cooperative `finish()` drain of accepted
/// buffered envelopes, `drainBarrier()` correctness under
/// concurrent producers, and post-accept append-failure routing
/// through ``FileLoggerDiagnostic/appendFailed(_:)``.
@Suite("FileLoggerWorker lifecycle + barrier")
struct FileLoggerWorkerLifecycleTests {
    private static func uniqueDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerFileTests")
            .appendingPathComponent(UUID().uuidString)
    }

    private static func cleanup(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }

    private static func tempExportURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerFileTests-export-\(UUID().uuidString).ndjson")
    }

    private static func canonicalLogRecord(
        message: String,
        level: LoggerLevel = .info,
        domain: LoggerDomain = "Lifecycle"
    ) -> LogRecord {
        LogRecord(
            timestamp: FileLogger.canonicalMillisecondDate(Date()),
            level: level,
            domain: domain,
            message: LogMessage(stringLiteral: message),
            attributes: []
        )
    }

    /// Sendable-safe accumulator for ``FileLoggerDiagnostic``
    /// events the lifecycle tests assert on.
    private final class DiagnosticRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var captured: [FileLoggerDiagnostic] = []

        func append(_ diagnostic: FileLoggerDiagnostic) {
            lock.lock()
            captured.append(diagnostic)
            lock.unlock()
        }

        var snapshot: [FileLoggerDiagnostic] {
            lock.lock()
            defer { lock.unlock() }
            return captured
        }
    }

    /// Deterministic ``FileLoggerAppendStore`` fake that throws
    /// a fixed typed ``FileLogStoreError`` on every append. Used
    /// to prove the worker's post-accept failure path without
    /// depending on host filesystem semantics. Backed by an
    /// actor so its append + attempt-counter state is serialized
    /// without needing an async-unsafe `NSLock`.
    private actor AlwaysFailingAppendStore: FileLoggerAppendStore {
        static let injectedError: FileLogStoreError =
            .invalidEnvelope(reason: .invalidContentType)

        private(set) var attempts: Int = 0

        func append(
            _: PersistentLogEnvelope
        ) async throws(FileLogStoreError) {
            attempts += 1
            throw Self.injectedError
        }
    }

    /// Deterministic ``FileLoggerAppendStore`` fake that suspends
    /// inside `append(_:)` until the test calls ``release()``,
    /// pinning the consumer task while the producer fills the
    /// bounded buffer. Lets the buffer-overflow test prove
    /// ``FileLoggerDiagnostic/bufferOverflow`` fires per dropped
    /// yield without relying on scheduler timing.
    ///
    /// `awaitEntered()` returns once the consumer's first
    /// `append(_:)` is in the suspended state; the producer can
    /// then enqueue further envelopes against a known buffer
    /// shape (consumer-pulled envelope held in `append`, buffer
    /// empty and ready to admit exactly `queueCapacity` more).
    private actor BlockingAppendStore: FileLoggerAppendStore {
        private var hasEntered = false
        private var isReleased = false
        private var entryWaiters: [CheckedContinuation<Void, Never>] = []
        private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

        func append(
            _: PersistentLogEnvelope
        ) async throws(FileLogStoreError) {
            if !hasEntered {
                hasEntered = true
                let waiters = entryWaiters
                entryWaiters.removeAll()
                for waiter in waiters { waiter.resume() }
            }
            guard !isReleased else { return }
            await withCheckedContinuation { continuation in
                releaseWaiters.append(continuation)
            }
        }

        func awaitEntered() async {
            if hasEntered { return }
            await withCheckedContinuation { continuation in
                entryWaiters.append(continuation)
            }
        }

        func release() {
            isReleased = true
            let waiters = releaseWaiters
            releaseWaiters.removeAll()
            for waiter in waiters { waiter.resume() }
        }
    }

    @Test("Worker `finish()` drains accepted buffered envelopes cooperatively (no cancel-after-finish)")
    func finishDrainsAcceptedBuffered() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true
        )
        // Build the store + worker directly so the test can
        // observe the persistent state after the worker tears
        // down. (Going through `FileLogger` would also tear down
        // its `FileLogStore` reference, and the export read-back
        // wouldn't have a handle to observe through.)
        let store = FileLogStore(
            configuration: FileLogStore.Configuration(directory: directory)
        )
        let worker = FileLoggerWorker(
            store: store,
            queueCapacity: 100,
            onDiagnostic: nil
        )

        let encoder = LogRecordPersistentEncoder()
        let envelopeCount = 5
        for index in 0 ..< envelopeCount {
            let envelope = try encoder.encode(
                Self.canonicalLogRecord(message: "buffered-\(index)")
            )
            worker.enqueue(envelope)
        }

        // Cooperative teardown: future yields drop, but the
        // already-accepted buffered envelopes must reach the
        // store before the consumer task exits.
        worker.finish()
        await worker.awaitDrainCompletion()

        try await store.flush()
        let exportURL = Self.tempExportURL()
        defer { Self.cleanup(exportURL) }
        try await store.exportLogs(to: exportURL)
        let bytes = try Data(contentsOf: exportURL)
        let lines = bytes.split(separator: 0x0A, omittingEmptySubsequences: true)
        // Every accepted envelope reached disk — `finish()`
        // didn't cancel the task mid-drain.
        #expect(lines.count == envelopeCount)
    }

    @Test("`drainBarrier()` waits for every accepted envelope under concurrent producers")
    func drainBarrierWaitsUnderConcurrentProducers() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        try FileManager.default.createDirectory(
            at: directory, withIntermediateDirectories: true
        )
        let store = FileLogStore(
            configuration: FileLogStore.Configuration(directory: directory)
        )
        let worker = FileLoggerWorker(
            store: store,
            queueCapacity: 10000,
            onDiagnostic: nil
        )

        let encoder = LogRecordPersistentEncoder()
        let envelopeCount = 200

        // Concurrent producers stress the yield + accepted-count
        // critical section. The barrier must observe every
        // `.enqueued` yield (regardless of which thread fired it)
        // and not return until the consumer task has appended
        // them all — a lost `recordDrained()` would either hang
        // the barrier or let it return early with envelopes still
        // in the bounded buffer.
        await withTaskGroup(of: Void.self) { group in
            for index in 0 ..< envelopeCount {
                group.addTask {
                    // Test inputs are encoder-safe; the `try?`
                    // only keeps this `addTask` closure
                    // non-throwing per
                    // `withTaskGroup(of: Void.self)`. The task
                    // body's role is to stress the worker's
                    // concurrent yield + accepted-count critical
                    // section, not to exercise encoder failure
                    // paths.
                    let record = Self.canonicalLogRecord(message: "concurrent-\(index)")
                    if let envelope = try? encoder.encode(record) {
                        worker.enqueue(envelope)
                    }
                }
            }
        }

        await worker.drainBarrier()
        try await store.flush()

        let exportURL = Self.tempExportURL()
        defer { Self.cleanup(exportURL) }
        try await store.exportLogs(to: exportURL)
        let bytes = try Data(contentsOf: exportURL)
        let lines = bytes.split(separator: 0x0A, omittingEmptySubsequences: true)
        // Every concurrent producer's `.enqueued` yield must
        // have been counted into `acceptedCount`, drained
        // through `FileLogStore.append(_:)`, and recorded via
        // `recordDrained()` before the barrier resumed.
        #expect(lines.count == envelopeCount)

        // Tear down cooperatively so the test does not leak the
        // detached consumer task.
        worker.finish()
        await worker.awaitDrainCompletion()
    }

    @Test("Post-accept append failure surfaces `.appendFailed` and still increments `recordDrained()`")
    func appendFailureSurfacesDiagnostic() async throws {
        // Deterministic append-failure injection through the
        // ``FileLoggerAppendStore`` seam. The fake throws a
        // fixed typed ``FileLogStoreError`` on every append, so
        // the test pins the exact error case the diagnostic
        // surfaces and proves the worker still calls
        // `recordDrained()` after the typed catch.
        let alwaysFailing = AlwaysFailingAppendStore()
        let diagnostics = DiagnosticRecorder()
        let worker = FileLoggerWorker(
            store: alwaysFailing,
            queueCapacity: 100,
            onDiagnostic: { diagnostics.append($0) }
        )

        let encoder = LogRecordPersistentEncoder()
        let envelope = try encoder.encode(
            Self.canonicalLogRecord(message: "append-fails")
        )
        worker.enqueue(envelope)

        // Wait for the consumer task to attempt the append and
        // observe the typed `FileLogStoreError` through the
        // diagnostic callback. `drainBarrier()` returning is
        // itself the proof that `recordDrained()` ran after the
        // typed catch: the barrier captures `acceptedCount` at
        // call time and only resumes once `drainedCount` catches
        // up.
        await worker.drainBarrier()

        let appendFailures = diagnostics.snapshot.filter {
            if case .appendFailed = $0 { return true }
            return false
        }
        try #require(appendFailures.count == 1)
        guard case let .appendFailed(error) = appendFailures[0] else {
            Issue.record("expected `.appendFailed` diagnostic")
            return
        }
        // The diagnostic carries the exact typed error the
        // fake injected — proving the worker's
        // `do throws(FileLogStoreError)` catch binding ran
        // verbatim, with no `any Error` widening or rewrap.
        #expect(error == AlwaysFailingAppendStore.injectedError)
        let attempts = await alwaysFailing.attempts
        #expect(attempts == 1)

        worker.finish()
        await worker.awaitDrainCompletion()
    }

    @Test("Bounded buffer overflow surfaces `.bufferOverflow` per dropped yield (deterministic)")
    func bufferOverflowFiresPerDroppedYield() async throws {
        // Deterministic overflow injection: a blocking append
        // store pins the consumer task inside `append(_:)` so the
        // bounded buffer fills to its known capacity and every
        // subsequent yield lands on the producer-side drop-newest
        // branch, firing `.bufferOverflow` synchronously.
        let blockingStore = BlockingAppendStore()
        let diagnostics = DiagnosticRecorder()
        let queueCapacity = 2
        let worker = FileLoggerWorker(
            store: blockingStore,
            queueCapacity: queueCapacity,
            onDiagnostic: { diagnostics.append($0) }
        )

        let encoder = LogRecordPersistentEncoder()
        let envelopes = try (0 ..< 10).map { index in
            try encoder.encode(
                Self.canonicalLogRecord(message: "overflow-\(index)")
            )
        }

        // Pre-encode + pre-enqueue one envelope so the consumer
        // task pulls it out of the buffer and reaches the
        // blocking `append(_:)`. Once `awaitEntered()` returns,
        // the consumer is suspended and the buffer is empty.
        worker.enqueue(envelopes[0])
        await blockingStore.awaitEntered()

        // Fill the buffer to capacity (`queueCapacity` admitted
        // yields), then issue `extraYields` more — every one of
        // those must hit the drop-newest branch and fire
        // `.bufferOverflow`. Consumer is parked inside
        // `append(_:)`, so the buffer never drains while these
        // yields run.
        let extraYields = 5
        for index in 1 ... queueCapacity + extraYields {
            worker.enqueue(envelopes[index])
        }

        let overflowEvents = diagnostics.snapshot.filter {
            if case .bufferOverflow = $0 { return true }
            return false
        }
        #expect(overflowEvents.count == extraYields)

        // Release the consumer + tear down cooperatively so the
        // detached task does not leak past the test.
        await blockingStore.release()
        worker.finish()
        await worker.awaitDrainCompletion()
    }
}
