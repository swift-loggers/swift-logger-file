import LoggerFilePersistence
import LoggerPersistence

/// Internal append-target seam for ``FileLoggerWorker``.
///
/// Decouples the worker's consumer task from a specific store
/// implementation so lifecycle tests can substitute a
/// deterministic fake that injects a typed ``FileLogStoreError``
/// on every append, instead of relying on
/// filesystem-permission-based failure injection that can drift
/// across host platforms.
///
/// Production callers route through ``FileLogStore``'s
/// conformance, declared in this file as an internal extension
/// — the protocol is not part of the public ``FileLogger`` API.
protocol FileLoggerAppendStore: Sendable {
    /// Appends `envelope` to the underlying store. Mirrors
    /// ``FileLogStore/append(_:)`` exactly so the worker's drain
    /// loop can pin the typed-throws contract with
    /// `do throws(FileLogStoreError)`.
    func append(
        _ envelope: PersistentLogEnvelope
    ) async throws(FileLogStoreError)
}

extension FileLogStore: FileLoggerAppendStore {}
