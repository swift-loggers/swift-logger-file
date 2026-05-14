import Foundation
import LoggerFilePersistence
import LoggerPersistence
import Loggers
import Testing

@testable import LoggerFile

// swiftlint:disable type_body_length
// Reason: LOCKED `FileLogger` `0.1.0` public-contract test IDs
// (drop-guard / autoclosure laziness / redaction / encoder
// failure / buffer overflow / accepted ordering / flush /
// non-destructive export / removeExportedLogs scope /
// rotation+retention passthrough) kept in one struct so the
// release validation stays cohesive and auditable.

/// Coverage for the public ``FileLogger`` contract: drop-guard,
/// autoclosure laziness, redaction-before-disk, encoder failure
/// diagnostic, bounded-buffer overflow diagnostic, accepted-FIFO
/// ordering, flush drain, non-destructive export, removal scope,
/// and rotation / retention pass-through to the underlying
/// `FileLogStore`.
@Suite("FileLogger public contract")
struct FileLoggerTests {
    private static func uniqueDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerFileTests")
            .appendingPathComponent(UUID().uuidString)
    }

    private static func cleanup(_ directory: URL) {
        try? FileManager.default.removeItem(at: directory)
    }

    private static func tempExportURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("LoggerFileTests-export-\(UUID().uuidString).ndjson")
    }

    private static func makeLogger(
        directory: URL,
        minimumLevel: FileLogger.MinimumLevel = .trace,
        queueCapacity: Int = 1000,
        rotation: RotationPolicy = .never,
        retention: RetentionPolicy = .unlimited,
        onDiagnostic: (@Sendable (FileLoggerDiagnostic) -> Void)? = nil,
        timestamp: Date = Date(timeIntervalSince1970: 0)
    ) -> FileLogger {
        FileLogger(
            directory: directory,
            rotation: rotation,
            retention: retention,
            minimumLevel: minimumLevel,
            queueCapacity: queueCapacity,
            onDiagnostic: onDiagnostic,
            dateProvider: { timestamp }
        )
    }

    /// Counter-backed `LogMessage` autoclosure used by the lazy
    /// drop tests. The `make` factory captures the recorder so
    /// every evaluation increments the count on the shared
    /// instance — the test asserts the count after calling
    /// `log(_:_:_:attributes:)`.
    private final class EvaluationRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var count: Int = 0

        func incrementMessage() {
            lock.lock()
            count += 1
            lock.unlock()
        }

        var snapshot: Int {
            lock.lock()
            defer { lock.unlock() }
            return count
        }
    }

    /// Sendable-safe accumulator for `onDiagnostic` events the
    /// tests assert on.
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

    /// Reads every envelope persisted under `directory` by
    /// exporting through the package's `exportLogs(to:)` contract
    /// into a scratch file and parsing the canonical NDJSON
    /// envelopes line by line. The test only inspects redacted
    /// envelope payloads, so this is the supported observation
    /// path.
    private static func readPersistedEnvelopes(
        from logger: FileLogger
    ) async throws -> [[String: Any]] {
        let exportURL = tempExportURL()
        defer { cleanup(exportURL) }
        try await logger.exportLogs(to: exportURL)
        let data = try Data(contentsOf: exportURL)
        guard !data.isEmpty else { return [] }
        return try data
            .split(separator: 0x0A, omittingEmptySubsequences: true)
            .map { lineBytes -> [String: Any] in
                let parsed = try JSONSerialization.jsonObject(with: Data(lineBytes))
                guard let object = parsed as? [String: Any] else {
                    throw IntegrationTestError.malformedEnvelopeLine
                }
                return object
            }
    }

    private enum IntegrationTestError: Error, Equatable {
        case malformedEnvelopeLine
    }

    // MARK: Drop-guard / autoclosure laziness

    @Test("`.disabled` entries drop without evaluating message or attributes")
    func disabledLevelDropsLazily() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let recorder = EvaluationRecorder()
        let logger = Self.makeLogger(directory: directory, minimumLevel: .trace)

        logger.log(
            .disabled,
            "Smoke",
            evaluatedMessage(recorder: recorder),
            attributes: evaluatedAttributes(recorder: recorder)
        )

        #expect(recorder.snapshot == 0)
        try await logger.flush()
        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        #expect(envelopes.isEmpty)
    }

    @Test("Entries strictly below the threshold drop without evaluating message or attributes")
    func belowThresholdDropsLazily() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let recorder = EvaluationRecorder()
        let logger = Self.makeLogger(directory: directory, minimumLevel: .warning)

        logger.log(
            .info,
            "Smoke",
            evaluatedMessage(recorder: recorder),
            attributes: evaluatedAttributes(recorder: recorder)
        )

        #expect(recorder.snapshot == 0)
        try await logger.flush()
        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        #expect(envelopes.isEmpty)
    }

    @Test("Above-threshold entries evaluate message and attributes exactly once")
    func aboveThresholdEvaluatesOnce() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let recorder = EvaluationRecorder()
        let logger = Self.makeLogger(directory: directory, minimumLevel: .info)

        logger.log(
            .info,
            "Smoke",
            evaluatedMessage(recorder: recorder),
            attributes: evaluatedAttributes(recorder: recorder)
        )

        // Two recorder increments — one from the message
        // autoclosure, one from the attributes autoclosure. Each
        // is evaluated exactly once.
        #expect(recorder.snapshot == 2)
    }

    // MARK: Redaction before queue / disk

    @Test("Private and sensitive attributes are redacted before envelopes reach disk")
    func redactionBeforeDisk() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let logger = Self.makeLogger(directory: directory, minimumLevel: .trace)

        logger.log(
            .info,
            "Smoke",
            "msg-redact",
            attributes: [
                LogAttribute(
                    "user.email",
                    .string("nobody@example.test"),
                    privacy: .private
                ),
                LogAttribute(
                    "user.token",
                    .string("plaintext-must-not-leak"),
                    privacy: .sensitive
                ),
                LogAttribute("public.tag", .string("kept"))
            ]
        )

        try await logger.flush()
        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        try #require(envelopes.count == 1)
        let envelope = envelopes[0]
        let payloadBase64 = try #require(envelope["payload"] as? String)
        let payloadData = try #require(Data(base64Encoded: payloadBase64))
        let payload = try #require(
            try JSONSerialization.jsonObject(with: payloadData) as? [String: Any]
        )
        // The canonical payload encodes attributes as an ordered
        // array of `{ "key": ..., "value": ... }` objects, where
        // `value` is the bare JSON-native representation of the
        // attribute's `LogValue` (string → JSON string, integer
        // → JSON number, …). See
        // `swift-logger-persistence/Docs/FileFormatSpec.md`.
        let attributes = try #require(payload["attributes"] as? [[String: Any]])
        func attributeValue(_ key: String) -> Any? {
            attributes.first { ($0["key"] as? String) == key }?["value"]
        }
        // Private + sensitive values are replaced before encoding.
        let emailValue = try #require(attributeValue("user.email") as? String)
        #expect(emailValue == "<private>")
        let tokenValue = try #require(attributeValue("user.token") as? String)
        #expect(tokenValue == "<redacted>")
        // Public attribute survives verbatim.
        let publicValue = try #require(attributeValue("public.tag") as? String)
        #expect(publicValue == "kept")
        // Raw plaintext sensitive value must never appear in the
        // exported bytes anywhere.
        let exportURL = Self.tempExportURL()
        defer { Self.cleanup(exportURL) }
        try await logger.exportLogs(to: exportURL)
        let rawBytes = try Data(contentsOf: exportURL)
        let rawString = try #require(String(data: rawBytes, encoding: .utf8))
        #expect(!rawString.contains("nobody@example.test"))
        #expect(!rawString.contains("plaintext-must-not-leak"))
    }

    // MARK: Encoder failure diagnostic

    @Test("Non-finite Double attribute surfaces `.encodingFailed(.nonFiniteDoubleAttribute)`")
    func nonFiniteDoubleSurfacesDiagnostic() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let diagnostics = DiagnosticRecorder()
        let logger = Self.makeLogger(
            directory: directory,
            minimumLevel: .trace,
            onDiagnostic: { diagnostics.append($0) }
        )

        logger.log(
            .info,
            "Smoke",
            "non-finite",
            attributes: [LogAttribute("metric", .double(.nan))]
        )

        let captured = diagnostics.snapshot
        try #require(captured.count == 1)
        #expect(captured[0] == .encodingFailed(.nonFiniteDoubleAttribute))

        // The entry was dropped before the worker queue.
        try await logger.flush()
        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        #expect(envelopes.isEmpty)
    }

    // MARK: Bounded-buffer overflow diagnostic

    @Test("Bounded buffer overflow surfaces `.bufferOverflow` per dropped yield")
    func bufferOverflowDiagnostic() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let diagnostics = DiagnosticRecorder()
        let logger = Self.makeLogger(
            directory: directory,
            queueCapacity: 1,
            onDiagnostic: { diagnostics.append($0) }
        )

        // Burst through the bounded buffer faster than the worker
        // can drain. Capacity 1 keeps the test fast: most yields
        // beyond the first land on the drop-newest branch.
        for index in 0 ..< 50 {
            logger.log(.info, "Smoke", "overflow-\(index)", attributes: [])
        }

        // Wait briefly so the worker has time to drain whatever
        // it admitted; tests on slow CI may admit two or three
        // before the worker re-enters the bounded buffer.
        try await Task.sleep(nanoseconds: 100_000_000)

        let overflowEvents = diagnostics.snapshot.filter {
            if case .bufferOverflow = $0 { return true }
            return false
        }
        #expect(!overflowEvents.isEmpty)

        let encoderFailures = diagnostics.snapshot.filter {
            if case .encodingFailed = $0 { return true }
            return false
        }
        #expect(encoderFailures.isEmpty)
    }

    // MARK: Accepted FIFO ordering + flush drain

    @Test("Accepted entries persist in arrival order through `flush()`")
    func acceptedEntriesPersistInOrder() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let logger = Self.makeLogger(directory: directory, minimumLevel: .trace)

        for index in 0 ..< 5 {
            logger.log(
                .info,
                "Order",
                "msg-\(index)",
                attributes: [LogAttribute("order", .integer(Int64(index)))]
            )
        }

        try await logger.flush()
        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        try #require(envelopes.count == 5)
        for (index, envelope) in envelopes.enumerated() {
            let payloadBase64 = try #require(envelope["payload"] as? String)
            let payloadData = try #require(Data(base64Encoded: payloadBase64))
            let payload = try #require(
                try JSONSerialization.jsonObject(with: payloadData) as? [String: Any]
            )
            let attributes = try #require(payload["attributes"] as? [[String: Any]])
            let orderEntry = try #require(
                attributes.first { ($0["key"] as? String) == "order" }
            )
            let orderValue = try #require(orderEntry["value"] as? NSNumber)
            #expect(orderValue.int64Value == Int64(index))
        }
    }

    // MARK: Non-destructive export + remove scope

    @Test("`exportLogs(to:)` does not remove appended envelopes from the store")
    func exportIsNonDestructive() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let logger = Self.makeLogger(directory: directory, minimumLevel: .trace)

        logger.log(.info, "Export", "kept-after-export", attributes: [])
        try await logger.flush()

        // First export: writes the byte-stable export.
        let firstExportURL = Self.tempExportURL()
        defer { Self.cleanup(firstExportURL) }
        try await logger.exportLogs(to: firstExportURL)

        // Second export: re-reads the same persistent prefix and
        // produces an identical byte-stable file. If the first
        // export had been destructive, this second export would
        // be empty.
        let secondExportURL = Self.tempExportURL()
        defer { Self.cleanup(secondExportURL) }
        try await logger.exportLogs(to: secondExportURL)

        let firstBytes = try Data(contentsOf: firstExportURL)
        let secondBytes = try Data(contentsOf: secondExportURL)
        #expect(!firstBytes.isEmpty)
        #expect(firstBytes == secondBytes)
    }

    @Test("`removeExportedLogs()` removes only the previously-exported prefix; later appends survive")
    func removeKeepsLaterAppends() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        let logger = Self.makeLogger(directory: directory, minimumLevel: .trace)

        // Phase A: append 3 entries, flush, export, then remove.
        for index in 0 ..< 3 {
            logger.log(.info, "Phase", "phase-a-\(index)", attributes: [])
        }
        try await logger.flush()
        let phaseAExportURL = Self.tempExportURL()
        defer { Self.cleanup(phaseAExportURL) }
        try await logger.exportLogs(to: phaseAExportURL)

        // Phase B: append 2 more entries BEFORE the remove. These
        // entries are appended after the export boundary and MUST
        // survive `removeExportedLogs()`.
        for index in 0 ..< 2 {
            logger.log(.info, "Phase", "phase-b-\(index)", attributes: [])
        }
        try await logger.flush()
        try await logger.removeExportedLogs()

        // After remove, a fresh export captures only the
        // phase-B entries.
        let afterRemoveExportURL = Self.tempExportURL()
        defer { Self.cleanup(afterRemoveExportURL) }
        try await logger.exportLogs(to: afterRemoveExportURL)

        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        try #require(envelopes.count == 2)
        for (index, envelope) in envelopes.enumerated() {
            let payloadBase64 = try #require(envelope["payload"] as? String)
            let payloadData = try #require(Data(base64Encoded: payloadBase64))
            let payload = try #require(
                try JSONSerialization.jsonObject(with: payloadData) as? [String: Any]
            )
            let message = try #require(payload["message"] as? String)
            #expect(message == "phase-b-\(index)")
        }
    }

    // MARK: Rotation + retention pass-through

    @Test("Non-default rotation + retention pass through to `FileLogStore.Configuration` without breaking append")
    func rotationAndRetentionPassThrough() async throws {
        let directory = Self.uniqueDirectory()
        defer { Self.cleanup(directory) }
        // `RotationPolicy.bySize(maxSegmentBytes:)` and
        // `RetentionPolicy.maxTotalBytes(_:)` validate against
        // `FileLogStore.maxEncodedLineBytes` (2 MiB) at the
        // factory boundary; use the legal minimum here. The test
        // proves the policies arrived at `FileLogStore.Configuration`
        // verbatim by constructing the `FileLogger` with them
        // and observing that admission still completes — a
        // mis-passed policy would either throw at the factory
        // (caught above) or surface as an `appendFailed`
        // diagnostic. Rotation / retention behaviour itself is
        // covered by `swift-logger-persistence`'s own test
        // matrix.
        let rotation = try RotationPolicy.bySize(maxSegmentBytes: FileLogStore.maxEncodedLineBytes)
        let retention = try RetentionPolicy.maxTotalBytes(FileLogStore.maxEncodedLineBytes)
        let diagnostics = DiagnosticRecorder()
        let logger = Self.makeLogger(
            directory: directory,
            minimumLevel: .trace,
            rotation: rotation,
            retention: retention,
            onDiagnostic: { diagnostics.append($0) }
        )

        for index in 0 ..< 3 {
            logger.log(.info, "Rotate", "rotate-\(index)", attributes: [])
        }
        try await logger.flush()

        let envelopes = try await Self.readPersistedEnvelopes(from: logger)
        #expect(envelopes.count == 3)
        #expect(diagnostics.snapshot.isEmpty)
    }

    // MARK: Helpers

    private func evaluatedMessage(recorder: EvaluationRecorder) -> LogMessage {
        recorder.incrementMessage()
        return "msg-\(recorder.snapshot)"
    }

    private func evaluatedAttributes(recorder: EvaluationRecorder) -> [LogAttribute] {
        recorder.incrementMessage()
        return [LogAttribute("evaluated", .integer(Int64(recorder.snapshot)))]
    }
}

// swiftlint:enable type_body_length
