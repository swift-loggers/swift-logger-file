import Foundation
import LoggerFilePersistence
import LoggerPersistence
import Loggers

/// A `Logger` adapter that redacts, persistently encodes, and
/// enqueues each allowed entry for caller-driven persistent
/// delivery to a local `FileLogStore`. `FileLogger` is local
/// only: there is no network, no remote retry, no remote
/// acknowledgement.
///
/// For each allowed entry the adapter:
///
/// 1. evaluates the `message` and `attributes` autoclosures
///    exactly once,
/// 2. builds a `LogRecord` stamped with a wall-clock timestamp,
/// 3. encodes the record synchronously through
///    `LogRecordPersistentEncoder` — redaction is the encoder's
///    first responsibility, so private and sensitive content is
///    replaced before anything crosses the worker queue or
///    touches disk, and
/// 4. enqueues the resulting `PersistentLogEnvelope` onto an
///    internal serial worker that calls
///    `FileLogStore.append(_:)` in accepted-FIFO order.
///
/// Entries strictly below the configured ``MinimumLevel`` and
/// entries at `LoggerLevel.disabled` are dropped without
/// evaluating the message or attributes autoclosures, and never
/// reach the encoder or the worker.
///
/// The worker's internal queue is **bounded**. When the consumer
/// task cannot keep up (slow disk, exhausted I/O) the worker
/// keeps the oldest accepted envelopes moving and **drops new
/// envelopes on the producer side** once the buffer hits its
/// capacity (1000 envelopes by default). The bounded-buffer drop
/// is observable through
/// ``FileLoggerDiagnostic/bufferOverflow``; encoder failures and
/// post-accept `FileLogStore.append(_:)` failures are observable
/// through ``FileLoggerDiagnostic/encodingFailed(_:)`` and
/// ``FileLoggerDiagnostic/appendFailed(_:)``. The diagnostic
/// callback is the only failure-surfacing surface; `log` itself
/// stays synchronous and infallible.
///
/// ## Caller-driven lifecycle
///
/// Persistence is durable — entries on disk survive process
/// restart — but the export and removal lifecycle is **caller
/// driven**:
///
/// - ``flush()`` drains the worker's accepted queue and calls
///   `FileLogStore.flush()` so every accepted envelope is
///   persisted before control returns.
/// - ``exportLogs(to:)`` drains the worker, flushes, and writes
///   a byte-stable export under `directory` via the persistence
///   package's export contract. Export is **non-destructive**:
///   the original segment files keep every appended envelope.
/// - ``removeExportedLogs()`` deletes only the persistence
///   bytes that were captured by the most recent successful
///   ``exportLogs(to:)`` boundary. Envelopes appended after the
///   export survive the removal.
///
/// `FileLogger` does not deliver to a remote sink itself. Hosts
/// that want remote durable delivery on top of the same
/// persistence layer use `swift-logger-remote`'s
/// `DurableRemoteQueue` + `RemoteEngine` instead.
public struct FileLogger: Loggers.Logger {
    /// A severity threshold for ``FileLogger``.
    ///
    /// `MinimumLevel` is intentionally severity-only and does not
    /// include a `disabled` case: per the `LoggerLevel` contract,
    /// `disabled` is a per-message sentinel and must not be used
    /// as a threshold value. To turn off logging entirely, use a
    /// logger that drops every entry instead of configuring a
    /// threshold.
    public enum MinimumLevel: CaseIterable, Sendable {
        /// The most detailed severity, intended for fine-grained
        /// tracing.
        case trace

        /// A detailed severity intended for debugging.
        case debug

        /// An informational severity describing normal operation.
        case info

        /// A normal but significant severity worth surfacing
        /// above everyday `info` traffic.
        case notice

        /// A severity for potential issues that do not yet stop
        /// execution.
        case warning

        /// A severity for error conditions that require
        /// attention.
        case error

        /// A severity for severe conditions that require
        /// immediate attention.
        case critical

        /// The default minimum severity used when none is
        /// specified.
        ///
        /// Equal to ``MinimumLevel/warning``.
        public static let defaultLevel = MinimumLevel.warning
    }

    /// The drop-guard threshold for this logger. Entries whose
    /// severity is strictly lower than this value -- and entries
    /// at `LoggerLevel.disabled` -- are dropped without
    /// evaluating the message or attributes autoclosures.
    public let minimumLevel: MinimumLevel

    private let dateProvider: @Sendable () -> Date
    private let encoder: LogRecordPersistentEncoder
    private let onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)?
    private let storage: FileLoggerStorage

    /// Creates a `FileLogger` that persists redacted records into
    /// a local `FileLogStore` rooted at `directory`.
    ///
    /// - Parameters:
    ///   - directory: The directory containing the persistent
    ///     log segment files. The store creates and manages the
    ///     directory; hosts SHOULD point this at an
    ///     application-controlled location (e.g. an
    ///     `Application Support` subdirectory) and own its
    ///     lifecycle across process restarts.
    ///   - rotation: Segment rotation policy applied by the
    ///     `FileLogStore`. Defaults to `.never`.
    ///   - retention: Segment retention policy applied by the
    ///     `FileLogStore` after each successful append.
    ///     Defaults to `.unlimited`.
    ///   - minimumLevel: The minimum severity to emit. Defaults
    ///     to ``MinimumLevel/defaultLevel``.
    ///   - queueCapacity: Upper bound on the worker's bounded
    ///     buffer. New yields beyond this capacity are dropped
    ///     and observed via
    ///     ``FileLoggerDiagnostic/bufferOverflow``. Defaults to
    ///     `1000`.
    ///   - onDiagnostic: Optional observer for
    ///     ``FileLoggerDiagnostic`` signals (encoder failures,
    ///     bounded-buffer overflow, post-accept append
    ///     failures). Fired synchronously on the producer thread
    ///     (or worker drain task for append failures); the
    ///     `log(_:_:_:attributes:)` contract stays synchronous
    ///     and infallible regardless.
    public init(
        directory: URL,
        rotation: RotationPolicy = .never,
        retention: RetentionPolicy = .unlimited,
        minimumLevel: MinimumLevel = .defaultLevel,
        queueCapacity: Int = 1000,
        onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)? = nil
    ) {
        self.init(
            directory: directory,
            rotation: rotation,
            retention: retention,
            minimumLevel: minimumLevel,
            queueCapacity: queueCapacity,
            onDiagnostic: onDiagnostic,
            dateProvider: { Date() }
        )
    }

    /// Test-only initializer that swaps the wall-clock source for
    /// an injected deterministic provider. Internal so production
    /// callers cannot depend on the seam.
    init(
        directory: URL,
        rotation: RotationPolicy = .never,
        retention: RetentionPolicy = .unlimited,
        minimumLevel: MinimumLevel = .defaultLevel,
        queueCapacity: Int = FileLoggerWorker.defaultQueueCapacity,
        onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)? = nil,
        dateProvider: @escaping @Sendable () -> Date
    ) {
        self.minimumLevel = minimumLevel
        self.dateProvider = dateProvider
        self.onDiagnostic = onDiagnostic
        encoder = LogRecordPersistentEncoder()
        let store = FileLogStore(
            configuration: FileLogStore.Configuration(
                directory: directory,
                rotation: rotation,
                retention: retention
            )
        )
        let worker = FileLoggerWorker(
            store: store,
            queueCapacity: queueCapacity,
            onDiagnostic: onDiagnostic
        )
        storage = FileLoggerStorage(store: store, worker: worker)
    }

    public func log(
        _ level: LoggerLevel,
        _ domain: LoggerDomain,
        _ message: @autoclosure @escaping @Sendable () -> LogMessage,
        attributes: @autoclosure @escaping @Sendable () -> [LogAttribute]
    ) {
        guard shouldEmit(level) else { return }
        let record = LogRecord(
            timestamp: dateProvider(),
            level: level,
            domain: domain,
            message: message(),
            attributes: attributes()
        )
        let envelope: PersistentLogEnvelope
        do {
            envelope = try encoder.encode(record)
        } catch {
            // The encoder applies redaction before encoding, so a
            // throw here means the encoder refused the record
            // shape itself (non-finite double, sequence
            // exhausted, …). The entry is dropped silently after
            // the diagnostic fires; the logger continues
            // processing later entries.
            onDiagnostic?(.encodingFailed(error))
            return
        }
        storage.worker.enqueue(envelope)
    }

    /// Drains the worker's accepted queue and flushes the
    /// underlying `FileLogStore` so every previously-accepted
    /// envelope is on disk before this call returns.
    public func flush() async throws {
        await storage.worker.drainBarrier()
        try await storage.store.flush()
    }

    /// Drains the worker, flushes the store, and writes a
    /// byte-stable export to `url` through the persistence
    /// package's export contract. The export is **non-
    /// destructive**: the original segment files keep every
    /// appended envelope.
    public func exportLogs(to url: URL) async throws {
        await storage.worker.drainBarrier()
        try await storage.store.flush()
        try await storage.store.exportLogs(to: url)
    }

    /// Removes only the persistence bytes captured by the most
    /// recent successful ``exportLogs(to:)`` boundary. Envelopes
    /// appended after that export survive the removal.
    public func removeExportedLogs() async throws {
        try await storage.store.removeExportedLogs()
    }

    /// Returns whether an entry at `level` passes the configured
    /// threshold. Pure and side-effect-free; used by
    /// ``log(_:_:_:attributes:)`` and exercised directly in
    /// tests.
    func shouldEmit(_ level: LoggerLevel) -> Bool {
        level != .disabled && level >= minimumLevel.asLoggerLevel
    }
}

/// Holds the actor-isolated `FileLogStore` + the serial worker
/// behind a reference type so `FileLogger`'s value semantics
/// share one storage instance across copies and across the
/// `Sendable` boundary `Loggers.Logger` consumers cross.
final class FileLoggerStorage: @unchecked Sendable {
    let store: FileLogStore
    let worker: FileLoggerWorker

    init(store: FileLogStore, worker: FileLoggerWorker) {
        self.store = store
        self.worker = worker
    }

    deinit {
        // Finish the worker's stream so the consumer task drains
        // any in-flight envelope and exits cooperatively.
        // Outstanding barriers resume immediately so a caller
        // awaiting `flush()` during teardown does not hang.
        worker.finish()
    }
}

extension FileLogger.MinimumLevel {
    fileprivate var asLoggerLevel: LoggerLevel {
        switch self {
        case .trace: return .trace
        case .debug: return .debug
        case .info: return .info
        case .notice: return .notice
        case .warning: return .warning
        case .error: return .error
        case .critical: return .critical
        }
    }
}
