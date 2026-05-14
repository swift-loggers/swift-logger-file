# swift-logger-file

Local persistent file logger for [`swift-loggers`](https://github.com/swift-loggers),
built on top of
[`swift-loggers/swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence).

`FileLogger` redacts each allowed entry, encodes it through
`LogRecordPersistentEncoder`, and persists the resulting
`PersistentLogEnvelope` to a host-owned `FileLogStore` directory.
The logger is **local only**: there is no network, no remote
retry, and no upload of any kind. Export is strictly
caller-driven through `exportLogs(to:)`. Persistent records
survive process restart and can be exported to a single
byte-stable NDJSON file through the caller-driven
`exportLogs(to:)` / `removeExportedLogs()` lifecycle for
diagnostic / support-bundle collection.

> **`0.1.0` public API surface (final, locked):**
>
> - `FileLogger(directory:rotation:retention:minimumLevel:queueCapacity:onDiagnostic:)`
>   — synchronous `Logger` conformance backed by an internal
>   serial worker and a bounded FIFO buffer with drop-newest
>   semantics.
> - `FileLogger.flush()` / `exportLogs(to:)` / `removeExportedLogs()`
>   — caller-driven persistent lifecycle.
> - `FileLoggerDiagnostic` — observable signals: `encodingFailed(_:)`,
>   `bufferOverflow`, `appendFailed(_:)`.
>
> `swift-logger-file` only logs locally. Hosts that need remote
> durable delivery on top of the same persistence layer use
> [`swift-logger-remote`](https://github.com/swift-loggers/swift-logger-remote)
> instead.

Requires Swift 6.0+. iOS 13.4, macOS 10.15.4, tvOS 13.4,
watchOS 6.2, visionOS 1. MIT licensed.

## Privacy

`FileLogger` invokes `LogRecordPersistentEncoder` synchronously
inside `log(_:_:_:attributes:)` before anything reaches the
worker queue or disk. The encoder applies the documented
redaction contract:

- `.public` message segments and attribute values are persisted
  verbatim.
- `.private` segments and attribute values are replaced with the
  literal `<private>`.
- `.sensitive` segments and attribute values are replaced with
  the literal `<redacted>`.

Raw plaintext for `.private` / `.sensitive` content never reaches
the bounded buffer or the persistent file. Exported support
bundles (`exportLogs(to:)` output) only contain redacted
envelopes — the raw plaintext was never written. Attribute keys,
record `domain`, and object keys are persisted verbatim by the
encoder; keep those names non-sensitive and PII-free.

## Installation

Add this package and the core `swift-loggers/swift-logger`
package (`Loggers`) to your `Package.swift`. Both products pin
to their `0.1.x` SemVer line through `.upToNextMinor(from: "0.1.0")`.

```swift
// In your Package.swift:
let package = Package(
    name: "MyApp",
    dependencies: [
        .package(
            url: "https://github.com/swift-loggers/swift-logger-file.git",
            .upToNextMinor(from: "0.1.0")
        ),
        .package(
            url: "https://github.com/swift-loggers/swift-logger.git",
            .upToNextMinor(from: "0.1.0")
        )
    ],
    targets: [
        .target(
            name: "MyApp",
            dependencies: [
                .product(name: "LoggerFile", package: "swift-logger-file"),
                .product(name: "Loggers", package: "swift-logger")
            ]
        )
    ]
)
```

## Usage

### Basic file logger

```swift
import Foundation
import LoggerFile
import Loggers

let logDirectory = FileManager.default.urls(
    for: .applicationSupportDirectory,
    in: .userDomainMask
)[0].appendingPathComponent("logs", isDirectory: true)

let logger: any Logger = FileLogger(
    directory: logDirectory,
    minimumLevel: .info
)

logger.info("Network", "user opened screen")
logger.error(
    "Auth",
    "sign-in failed",
    attributes: [LogAttribute("reason", .string("expired-token"))]
)
```

`Logger.log` stays synchronous and infallible. Entries strictly
below the configured `minimumLevel` (and entries at
`LoggerLevel.disabled`) are dropped without evaluating the
message or attributes autoclosures.

### Caller-driven lifecycle: support-bundle export

```swift
import Foundation
import LoggerFile

func collectSupportBundle(
    logger: FileLogger,
    destination: URL
) async throws {
    // `flush()` drains the worker's accepted queue and flushes
    // the underlying `FileLogStore` so every accepted envelope
    // is on disk before control returns.
    try await logger.flush()

    // `exportLogs(to:)` writes a single byte-stable NDJSON file
    // at `destination`. Export is non-destructive: the
    // persistent segment files keep every appended envelope, so
    // the host can ship the export file and keep accruing new
    // entries.
    try await logger.exportLogs(to: destination)

    // `removeExportedLogs()` deletes only the persistence bytes
    // that were captured by the most recent successful
    // `exportLogs(to:)` boundary after the caller confirms export
    // handling. Envelopes appended after the export survive the
    // removal.
    try await logger.removeExportedLogs()
}
```

The exported file is a sequence of canonical
`PersistentLogEnvelope` JSON lines (NDJSON), each carrying a
redacted payload. Consumers ship the file as-is; no further
decoding is required to support a diagnostic workflow.

### Observing diagnostics

```swift
import Foundation
import LoggerFile
import Loggers

let logDirectory = FileManager.default.urls(
    for: .applicationSupportDirectory,
    in: .userDomainMask
)[0].appendingPathComponent("logs", isDirectory: true)

let counter = OverflowCounter()
let logger = FileLogger(
    directory: logDirectory,
    minimumLevel: .info,
    queueCapacity: 1000,
    onDiagnostic: { diagnostic in
        switch diagnostic {
        case let .encodingFailed(error):
            // The encoder refused the record shape (e.g. a
            // non-finite `LogValue.double`). The entry is already
            // dropped; this callback only observes.
            counter.recordEncodingFailure(error)
        case .bufferOverflow:
            // Drop-newest applied at the bounded buffer. Surface
            // it as a metric — `Logger.log` is synchronous and
            // never throws.
            counter.recordOverflow()
        case let .appendFailed(error):
            // `FileLogStore.append(_:)` threw after the envelope
            // was already admitted to the worker. The envelope
            // is dropped without replay; the logger keeps
            // processing later entries on the same instance.
            counter.recordAppendFailure(error)
        }
    }
)

// Thread-safe sink: concurrent `Logger.log` calls may invoke the
// callback concurrently, so the host MUST guard shared state.
final class OverflowCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var overflowCount: Int = 0
    private var encodingFailureCount: Int = 0
    private var appendFailureCount: Int = 0

    func recordOverflow() {
        lock.lock(); defer { lock.unlock() }
        overflowCount += 1
    }
    func recordEncodingFailure(_: Error) {
        lock.lock(); defer { lock.unlock() }
        encodingFailureCount += 1
    }
    func recordAppendFailure(_: Error) {
        lock.lock(); defer { lock.unlock() }
        appendFailureCount += 1
    }
}
```

`Logger.log` stays synchronous and infallible regardless of which
signals fire. The caller-driven lifecycle methods —
`flush()`, `exportLogs(to:)`, and `removeExportedLogs()` — are
explicit `async throws` calls and may suspend on persistence
I/O (worker drain, `FileLogStore.flush()`, export write,
removal compaction); the synchronous-and-infallible guarantee
applies only to `Logger.log`. Diagnostics are advisory; they do
not change the adapter's drop-newest contract.

## Related packages

- [`swift-loggers/swift-logger`](https://github.com/swift-loggers/swift-logger)
  — the core ecosystem package: `Logger` protocol, `LogRecord`,
  privacy primitives, companion adapters (`LoggerPrint`,
  `LoggerFiltering`, `LoggerNoOp`).
- [`swift-loggers/swift-logger-persistence`](https://github.com/swift-loggers/swift-logger-persistence)
  — durable record persistence: `LogRecordPersistentEncoder`,
  `PersistentLogEnvelope`, `FileLogStore`, rotation / retention
  policies, byte-stable export contract.
- [`swift-loggers/swift-logger-remote`](https://github.com/swift-loggers/swift-logger-remote)
  — sink-neutral durable remote-delivery engine on top of
  `swift-logger-persistence`. Use this package when remote
  delivery (retry, batch transport, acknowledgement-to-removal
  lifecycle) is required.
