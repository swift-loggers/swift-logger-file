// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "swift-logger-file",
    platforms: [
        // Aligned with `swift-logger-persistence` minimums so the
        // `FileLogStore` actor + `LogRecordPersistentEncoder` link
        // against the consumer surface without bumping platform
        // floors on hosts.
        .iOS("13.4"),
        .tvOS("13.4"),
        .macOS("10.15.4"),
        .watchOS("6.2"),
        .visionOS(.v1)
    ],
    products: [
        .library(
            name: "LoggerFile",
            targets: ["LoggerFile"]
        )
    ],
    dependencies: [
        .package(
            url: "https://github.com/swift-loggers/swift-logger.git",
            .upToNextMinor(from: "0.1.0")
        ),
        .package(
            url: "https://github.com/swift-loggers/swift-logger-persistence.git",
            .upToNextMinor(from: "0.1.0")
        ),
        .package(url: "https://github.com/apple/swift-docc-plugin.git", from: "1.0.0")
    ],
    targets: [
        .target(
            name: "LoggerFile",
            dependencies: [
                .product(name: "Loggers", package: "swift-logger"),
                .product(name: "LoggerPersistence", package: "swift-logger-persistence"),
                .product(name: "LoggerFilePersistence", package: "swift-logger-persistence")
            ]
        ),
        .testTarget(
            name: "LoggerFileTests",
            dependencies: [
                "LoggerFile",
                .product(name: "Loggers", package: "swift-logger"),
                .product(name: "LoggerPersistence", package: "swift-logger-persistence"),
                .product(name: "LoggerFilePersistence", package: "swift-logger-persistence")
            ]
        )
    ]
)
