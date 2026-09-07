# Compression implementation

This document connects the [compression specification](../compression.md) to the implementation in
this repository. It is informative: deviations recorded here do not weaken the generic contract.

## Public API

Request compression is configured with
[`RequestCompressionAlgorithm`](../../src/main/java/com/scylladb/alternator/RequestCompressionAlgorithm.java)
and `minCompressionSizeBytes` in
[`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java). Response
compression is configured with
[`ResponseCompressionAlgorithm`](../../src/main/java/com/scylladb/alternator/ResponseCompressionAlgorithm.java).

Both client builders propagate these settings and install the corresponding execution interceptors.

## Internal architecture

[`GzipRequestInterceptor`](../../src/main/java/com/scylladb/alternator/GzipRequestInterceptor.java)
reads and caches serialized request bytes, decides whether the threshold is met, and provides a
replacement blocking or asynchronous body.

[`ResponseCompressionInterceptor`](../../src/main/java/com/scylladb/alternator/ResponseCompressionInterceptor.java)
adds `Accept-Encoding`, recognizes configured response encodings, strips stale response headers,
and wraps blocking streams or asynchronous publishers for decompression.

## Lifecycle and concurrency

Compression runs in the request and response interceptor lifecycle. Request bodies are buffered
before compression. Asynchronous responses are also buffered completely, then emitted as one
decoded buffer after positive demand. Per-request execution attributes hold cached request bytes,
the request compression decision, and the selected response encoding.

## Requirement mapping

| Requirement | Code | Test evidence | Status |
| --- | --- | --- | --- |
| `COMP-REQ-001` | [`AlternatorConfig`](../../src/main/java/com/scylladb/alternator/AlternatorConfig.java) | [`FeatureSpecDefaultsTest#compressionDefaultsMatchSpecification`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java) | `conformant` |
| `COMP-REQ-002` | [`GzipRequestInterceptor`](../../src/main/java/com/scylladb/alternator/GzipRequestInterceptor.java) | [`GzipRequestInterceptorTest#testCompressedBodyIsValidGzip`](../../src/test/java/com/scylladb/alternator/GzipRequestInterceptorTest.java) | `gap` |
| `COMP-REQ-003` | [`GzipRequestInterceptor`](../../src/main/java/com/scylladb/alternator/GzipRequestInterceptor.java) | — | `gap` |
| `COMP-REQ-004` | [`ResponseCompressionInterceptor`](../../src/main/java/com/scylladb/alternator/ResponseCompressionInterceptor.java) | [`ResponseCompressionInterceptorTest#testGzipSyncResponseIsDecompressedAndHeadersAreStripped`](../../src/test/java/com/scylladb/alternator/ResponseCompressionInterceptorTest.java) | `gap` |
| `COMP-REQ-005` | [`ResponseCompressionInterceptor`](../../src/main/java/com/scylladb/alternator/ResponseCompressionInterceptor.java) | — | `gap` |
| `COMP-REQ-006` | [`ResponseCompressionInterceptor`](../../src/main/java/com/scylladb/alternator/ResponseCompressionInterceptor.java) | [`ResponseCompressionInterceptorTest#testGzipAsyncResponseIsDecompressedAndHeadersAreStripped`](../../src/test/java/com/scylladb/alternator/ResponseCompressionInterceptorTest.java) | `gap` |
| `COMP-REQ-007` | [`AlternatorDynamoDbClient`](../../src/main/java/com/scylladb/alternator/AlternatorDynamoDbClient.java) | [`AlternatorConfigHeadersTest#testHeadersOptimizationWithCompression`](../../src/test/java/com/scylladb/alternator/AlternatorConfigHeadersTest.java) | `gap` |
| `COMP-REQ-008` | [`AlternatorDynamoDbAsyncClient`](../../src/main/java/com/scylladb/alternator/AlternatorDynamoDbAsyncClient.java) | [`ResponseCompressionIT#testAsyncSdkClientParsesGzipResponse`](../../src/integration-test/java/com/scylladb/alternator/ResponseCompressionIT.java) | `gap` |

## Test coverage

- [`FeatureSpecDefaultsTest`](../../src/test/java/com/scylladb/alternator/FeatureSpecDefaultsTest.java)
  executes the machine-readable compression defaults.
- [`RequestCompressionAlgorithmTest`](../../src/test/java/com/scylladb/alternator/RequestCompressionAlgorithmTest.java)
  covers request algorithm values and enablement.
- [`GzipRequestInterceptorTest`](../../src/test/java/com/scylladb/alternator/GzipRequestInterceptorTest.java)
  covers direct interceptor-method thresholds, gzip round trips, and blocking/asynchronous bodies.
- [`ResponseCompressionInterceptorTest`](../../src/test/java/com/scylladb/alternator/ResponseCompressionInterceptorTest.java)
  covers negotiation, gzip/deflate decoding, header stripping, unsupported encodings, and a
  single-buffer asynchronous success path.
- [`ResponseCompressionIT`](../../src/integration-test/java/com/scylladb/alternator/ResponseCompressionIT.java)
  verifies end-to-end protocol parsing for compressed blocking and asynchronous responses.
- [`AlternatorDynamoDbClientCustomizerTest`](../../src/test/java/com/scylladb/alternator/AlternatorDynamoDbClientCustomizerTest.java)
  and
  [`AlternatorDynamoDbAsyncClientCustomizerTest`](../../src/test/java/com/scylladb/alternator/AlternatorDynamoDbAsyncClientCustomizerTest.java)
  cover builder propagation.

## Known conformance gaps

- `COMP-REQ-002`, `COMP-REQ-003`, `COMP-REQ-007`, `COMP-REQ-008`: The request-compression
  interceptor is not conformant in the current request pipeline. Content modifiers run before
  `modifyHttpRequest(...)`, but the interceptor reads the body and records its decision only in
  `modifyHttpRequest(...)`. An ordinary eligible request therefore carries `Content-Encoding:
  gzip` with its original JSON bytes. Existing tests call these methods in the opposite order and
  integration tests inspect the header without validating the transmitted gzip stream.
- `COMP-REQ-002`: A replacement body does not update the original `Content-Length`, and a present
  zero-byte body is compressed when the threshold is zero.
- `COMP-REQ-003`: Compressor failure returns original bytes after the gzip header decision instead
  of failing or removing that header. No injected compressor-failure test covers the path.
- `COMP-REQ-004`: The selected response encoding is stored in request-scoped attributes and is not
  cleared for a later unencoded, unsupported, or ambiguous retry response. A stale encoding can be
  reused across attempts.
- `COMP-REQ-005`, `COMP-REQ-006`, `COMP-REQ-008`: Corrupt configured responses, asynchronous demand
  accounting, non-positive demand, upstream errors, cancellation, case and parameter handling,
  duplicate algorithms, and multiple encoding header values lack direct coverage.
