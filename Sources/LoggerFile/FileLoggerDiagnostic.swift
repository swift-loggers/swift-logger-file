import Foundation
import LoggerFilePersistence
import LoggerPersistence

/// Observable diagnostic signal emitted by ``FileLogger``. Hosts
/// opt into observation by passing an `onDiagnostic` callback to
/// ``FileLogger/init(directory:rotation:retention:minimumLevel:queueCapacity:onDiagnostic:)``.
///
/// The callback runs synchronously on the producer thread that
/// triggered the signal (the thread calling
/// ``FileLogger/log(_:_:_:attributes:)`` for
/// ``encodingFailed(_:)`` / ``bufferOverflow``, or the worker
/// drain task for ``appendFailed(_:)``). Hosts SHOULD route
/// diagnostics elsewhere without blocking inside the callback.
///
/// **Concurrency.** Concurrent ``FileLogger/log(_:_:_:attributes:)``
/// calls may invoke `onDiagnostic` concurrently from multiple
/// threads. Host diagnostic sinks MUST be reentrant and
/// thread-safe (e.g. guard shared counters or arrays with a
/// lock); being non-blocking is necessary but not sufficient.
///
/// ``FileLogger/log(_:_:_:attributes:)`` itself remains synchronous
/// and infallible regardless of which signals fire.
public enum FileLoggerDiagnostic: Sendable, Equatable {
    /// `LogRecordPersistentEncoder.encode(_:)` failed on the
    /// host-supplied record (e.g. a `LogValue.double` carrying a
    /// non-finite value). The entry is dropped silently after
    /// this signal fires; the logger continues processing later
    /// entries.
    case encodingFailed(LogRecordPersistentEncoderError)

    /// The worker's bounded FIFO buffer rejected a yield because
    /// it had reached capacity. Drop-newest semantics apply: the
    /// rejected envelope never reaches `FileLogStore.append(_:)`.
    case bufferOverflow

    /// `FileLogStore.append(_:)` threw on a previously-accepted
    /// envelope. The envelope is dropped silently after this
    /// signal fires; the logger continues processing later
    /// entries on the same `FileLogger` instance.
    case appendFailed(FileLogStoreError)
}
