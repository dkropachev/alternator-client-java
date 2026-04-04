# Alternator - Client-side load balancing - Java

## Introduction
DynamoDB applications
are usually aware of a _single endpoint_, a single URL to which they
connect - e.g., `http://dynamodb.us-east-1.amazonaws.com`. But Alternator
is distributed over a cluster of nodes and we would like the application to
send requests to all these nodes - not just to one. This is important for two
reasons: **high availability** (the failure of a single Alternator node should
not prevent the client from proceeding) and **load balancing** over all
Alternator nodes.

One of the ways to do this is to provide a modified library, which will
allow a mostly-unmodified application which is only aware of one
"endpoint URL" to send its requests to many different Alternator nodes.

Our intention is _not_ to fork the existing AWS client library (SDK) for Java.
Rather, our intention is to provide a tiny library which tacks on to any
version of the AWS SDK that the application is already using, and makes
it do the right thing for Alternator.

This library supports AWS SDK for Java Version 2 (requires 2.20 or above) and requires Java 11 or later.

## Add `load-balancing` to your project

### Maven Dependency

Add the `load-balancing` dependency to your Maven project by adding the
following `dependency` to your `pom.xml` definition:

~~~ xml
<dependency>
  <groupId>com.scylladb.alternator</groupId>
  <artifactId>load-balancing</artifactId>
  <version>2.0.0</version>
</dependency>
~~~

You can find the latest version [here](https://central.sonatype.com/artifact/com.scylladb.alternator/load-balancing).

### Alternatively, build the LoadBalancing jar
To build a jar of the Alternator client-side load balancer, use
```
mvn package
```
Which creates `target/load-balancing-2.0.1-SNAPSHOT.jar`.

## HTTP Client Configuration

The AWS SDK for Java v2 separates HTTP client implementations from the core SDK.
This library works with any of the supported implementations — you choose which
one to include in your project based on your needs.

### Available implementations

| Implementation | Maven Artifact | Sync | Async | Best for |
|---|---|:---:|:---:|---|
| **Apache HttpClient** | `apache-client` | Yes | — | Sync workloads, familiar API, mature ecosystem |
| **Netty NIO** | `netty-nio-client` | — | Yes | Async workloads, high concurrency, non-blocking I/O |
| **AWS CRT** | `aws-crt-client` | Yes | Yes | Both sync and async, native performance via C libraries |

**Apache** and **Netty** are the most common choices. Apache is the SDK's traditional
sync client, and Netty is its traditional async client. **AWS CRT** is a newer alternative
that supports both sync and async through native bindings — it can replace either Apache
or Netty (or both), but requires platform-specific native libraries.

### Adding an HTTP client dependency

Add **at least one** HTTP client dependency to your project. Which one depends on whether
you use the sync API, the async API, or both:

**Sync API only** — choose Apache (recommended) or CRT:
```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>apache-client</artifactId>
</dependency>
```

**Async API only** — choose Netty (recommended) or CRT:
```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>netty-nio-client</artifactId>
</dependency>
```

**Both sync and async** — add one sync and one async implementation, or use CRT for both:
```xml
<!-- Option A: Apache (sync) + Netty (async) -->
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>apache-client</artifactId>
</dependency>
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>netty-nio-client</artifactId>
</dependency>

<!-- Option B: CRT handles both -->
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>aws-crt-client</artifactId>
</dependency>
```

When multiple implementations are on the classpath, the library auto-detects and
prefers Apache for sync and Netty for async (falling back to CRT if those are absent).

### Customizing the HTTP client

The library creates HTTP clients internally with Alternator-optimized defaults
(connection pool size, idle timeouts, TCP keep-alive, HTTP keep-alive). To
customize these settings, use the implementation-specific customizer callbacks:

```java
// Customize the Apache HTTP client (sync)
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .withApacheHttpClientCustomizer(builder -> builder
        .maxConnections(200)
        .connectionTimeout(Duration.ofSeconds(5)))
    .build();

// Customize the CRT HTTP client (sync)
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .withCrtHttpClientCustomizer(builder -> builder
        .maxConcurrency(200))
    .build();

// Customize the Netty HTTP client (async)
DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .withNettyHttpClientCustomizer(builder -> builder
        .maxConcurrency(200)
        .connectionTimeout(Duration.ofSeconds(5)))
    .build();

// Customize the CRT HTTP client (async)
DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .withCrtAsyncHttpClientCustomizer(builder -> builder
        .maxConcurrency(200))
    .build();
```

The customizer receives the builder **after** all Alternator defaults and TLS settings
have been applied, so you only need to override the settings you want to change. The
customizer is the last thing called before `build()`, giving you full control over the
final builder state.

**Note:** Customizers are mutually exclusive with `httpClient()` / `httpClientBuilder()`.
If you provide a fully custom HTTP client via those methods, customizers cannot be used
(and the library will not apply its optimized defaults to your client).

### Connection pool tuning

The library applies Alternator-optimized connection pool defaults out of the box.
For common pool settings that apply regardless of which HTTP client implementation is
used, you can configure them directly on the builder:

```java
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .withMaxConnections(200)
    .withConnectionMaxIdleTimeMs(30000)
    .withConnectionTimeToLiveMs(60000)  // Apache and Netty only; ignored by CRT
    .withConnectionAcquisitionTimeoutMs(5000)
    .withConnectionTimeoutMs(3000)
    .build();
```

| Setting | Default | Description |
|---------|---------|-------------|
| `maxConnections` | 400 | Maximum connections in the pool. Higher than SDK defaults to support multi-node clusters. |
| `connectionMaxIdleTimeMs` | 600,000 (10 min) | Idle connections are closed after this time. Set to 0 to disable idle eviction (a warning is logged). |
| `connectionTimeToLiveMs` | 0 (unlimited) | Maximum lifetime for any connection. Apache and Netty only; CRT logs a warning and ignores it. |
| `connectionAcquisitionTimeoutMs` | 10,000 (10 sec) | How long to wait for a connection from an exhausted pool. |
| `connectionTimeoutMs` | 15,000 (15 sec) | How long to wait for a TCP connection to be established. |

**CRT HTTP client note:** The CRT SDK requires positive durations for timeouts. Setting
`connectionAcquisitionTimeoutMs` or `connectionTimeoutMs` to 0 with CRT will fall back to
SDK defaults (an info message is logged).

These settings are applied as defaults before any customizer callback runs, so the
customizer can override them if needed.

### Keep-alive defaults

The library enables both TCP and HTTP keep-alive by default on all HTTP clients to
prevent firewalls and NATs from silently dropping idle connections.

**TCP keep-alive** (`SO_KEEPALIVE`) is enabled on every HTTP client:
- Apache and Netty: `tcpKeepAlive(true)` — uses OS-level TCP keep-alive settings
- AWS CRT: `TcpKeepAliveConfiguration` with 30-second interval and 30-second timeout

**HTTP keep-alive** (`Connection: keep-alive` header) is added to every DynamoDB request
by the query plan interceptor, signaling to the server that the connection should be
reused for subsequent requests.

Both mechanisms work together to maintain persistent, reusable connections across all
Alternator nodes, reducing connection setup overhead and improving throughput.

## Usage

As explained above, this package does not _replace_ the AWS SDK for Java, but
accompanies it. The package provides a new mechanism to configure a
DynamoDbClient object, which the application can then use normally using the
standard AWS SDK for Java, to make requests. The load balancer library ensures
that each of these requests goes to a different live Alternator node.

The load balancer library is also responsible for _discovering_ which
Alternator nodes exist, and maintaining this list as the Alternator cluster
changes. It does this using an additional background thread, which
periodically polls one of the known nodes, asking it for a list of all other
nodes (in this data-center).

### Using the library

An application creates a `DynamoDbClient` object
and then uses it to perform various requests. Traditionally, to create such
an object, an application that wishes to connect to a specific URL would use
code that looks something like this:

```java
    static AwsCredentialsProvider myCredentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));
    URI uri = URI.create("https://127.0.0.1:8043");
    DynamoDbClient client = DynamoDbClient.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(uri)
        .credentialsProvider(myCredentials)
        .build();
```

The `region()` chosen doesn't matter when the endpoint is explicitly chosen
with `endpointOverride()`, but nevertheless should be specified otherwise the
SDK will try to look it up in a configuration file, and complain if it isn't
set there.

#### Using `AlternatorDynamoDbClient`

The simplest way to use the Alternator load balancer is to use the `AlternatorDynamoDbClient.builder()`,
which provides a familiar builder API that implements `DynamoDbClientBuilder`:

```java
    import com.scylladb.alternator.AlternatorDynamoDbClient;

static AwsCredentialsProvider myCredentials =
    StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .build();
```

The `AlternatorDynamoDbClient` automatically integrates query plan interceptors
to provide load balancing across all Alternator nodes.

The application can then use this `DynamoDbClient` object completely normally,
just that each request will go to a different Alternator node, instead of all
of them going to the same URL.

The parameter `uri` is one known Alternator node, which is then contacted to
discover the rest. After this initialization, this original node may go down
at any time - any other already-known node can be used to retrieve the node
list, and we no longer rely on the original node.

You can see `src/integration-test/java/com/scylladb/alternator/demo/Demo2.java` for a
complete example of using client-side load balancing with AWS SDK for Java.
After building with `mvn package`, you can run this demo with the command:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo2 -Dexec.classpathScope=test
```

#### Asynchronous operation

You can achieve better scalability and performance using the asynchronous
versions of API calls and `java.util.concurrent` completion chaining.

##### Using `AlternatorDynamoDbAsyncClient`

The simplest way to create a `DynamoDbAsyncClient` with Alternator load balancing is to use
the `AlternatorDynamoDbAsyncClient.builder()`:

```java
    import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;

static AwsCredentialsProvider myCredentials =
    StaticCredentialsProvider.create(AwsBasicCredentials.create("myuser", "mypassword"));

DynamoDbAsyncClient client = AlternatorDynamoDbAsyncClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .build();
```

You can see `src/integration-test/java/com/scylladb/alternator/demo/Demo3.java` for a
complete example of using client-side load balancing with the asynchronous API.
After building with `mvn package`, you can run this demo with the command:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo3 -Dexec.classpathScope=test
```

### Using DynamoDB Enhanced Client

The [DynamoDB Enhanced Client](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/dynamodb-enhanced-client.html)
provides a high-level, object-mapping interface that simplifies working with DynamoDB tables
by mapping Java classes to table items. Since the enhanced client wraps the standard
`DynamoDbClient`, load balancing works transparently.

#### Synchronous Enhanced Client

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
public class Customer {
    private String id;
    private String name;

    public Customer() {}
    public Customer(String id, String name) { this.id = id; this.name = name; }

    @DynamoDbPartitionKey
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}

// 1. Create standard DynamoDbClient with load balancing
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .build();

// 2. Wrap with DynamoDbEnhancedClient
DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
    .dynamoDbClient(client)
    .build();

// 3. Use the enhanced client normally
DynamoDbTable<Customer> customerTable = enhancedClient.table(
    "customers",
    TableSchema.fromBean(Customer.class)
);
customerTable.putItem(new Customer("123", "John Doe"));
```

You can see `src/integration-test/java/com/scylladb/alternator/demo/Demo4.java` for a
complete example. After building with `mvn package`, you can run this demo with:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo4 -Dexec.classpathScope=test
```

#### Asynchronous Enhanced Client

```java
import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;

// 1. Create async DynamoDbAsyncClient with load balancing
DynamoDbAsyncClient asyncClient = AlternatorDynamoDbAsyncClient.builder()
    .endpointOverride(URI.create("http://localhost:8043"))
    .credentialsProvider(myCredentials)
    .build();

// 2. Wrap with DynamoDbEnhancedAsyncClient
DynamoDbEnhancedAsyncClient enhancedAsyncClient = DynamoDbEnhancedAsyncClient.builder()
    .dynamoDbClient(asyncClient)
    .build();

// 3. Use the enhanced async client with CompletableFuture
DynamoDbAsyncTable<Customer> customerTable = enhancedAsyncClient.table(
    "customers",
    TableSchema.fromBean(Customer.class)
);
customerTable.putItem(new Customer("123", "John Doe"))
    .thenAccept(v -> System.out.println("Item saved"))
    .join();
```

You can see `src/integration-test/java/com/scylladb/alternator/demo/Demo5.java` for a
complete example. After building with `mvn package`, you can run this demo with:
```
mvn exec:java -Dexec.mainClass=com.scylladb.alternator.demo.Demo5 -Dexec.classpathScope=test
```

### Request Compression

The library supports optional GZIP compression for HTTP request bodies, which can
reduce network bandwidth usage when sending large payloads to Alternator.

#### Why not use AWS SDK's built-in compression?

AWS SDK for Java v2 includes a `CompressionConfiguration` feature, but it **only works
for AWS services that have the `@requestCompression` trait** in their service model
(e.g., CloudWatch's `PutMetricData`). DynamoDB does not support this trait, so the
SDK's built-in compression cannot be used for DynamoDB or Alternator requests.

This library provides its own compression implementation using an `ExecutionInterceptor`
that works with any DynamoDB/Alternator operation.

#### Enabling compression

To enable request compression, configure the builder with a compression algorithm:

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.RequestCompressionAlgorithm;

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .build();
```

#### Compression threshold

By default, only requests with bodies >= 1024 bytes (1 KB) are compressed. This avoids
the overhead of compressing small payloads that may not benefit from compression.
You can customize this threshold:

```java
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withMinCompressionSizeBytes(2048)  // Only compress requests >= 2KB
    .build();
```

#### When to use compression

Request compression is recommended for:
- Large item attributes (documents, JSON blobs)
- Batch operations with many items (`BatchWriteItem`, `BatchGetItem`)
- Text-heavy data that compresses well

Request compression may not be beneficial for:
- Small requests (< 1KB)
- Already-compressed binary data
- Low-latency requirements where CPU overhead matters

### Headers Optimization

The library supports optional HTTP headers optimization, which reduces network bandwidth by
removing headers that Alternator does not use. According to benchmarks, this can reduce
outgoing traffic by up to 56% depending on workload and encryption.

#### Enabling headers optimization

To enable headers optimization, use `withOptimizeHeaders(true)` on the builder:

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withOptimizeHeaders(true)
    .build();
```

#### Default headers whitelist

When headers optimization is enabled, only the following headers are preserved by default:
- `Host` - Required by HTTP/1.1
- `X-Amz-Target` - Specifies the DynamoDB operation
- `Content-Type` - MIME type for DynamoDB API
- `Content-Length` - Required for request body
- `Accept-Encoding` - For response compression negotiation
- `Content-Encoding` - For request compression (when enabled)
- `Authorization` - AWS SigV4 signature (when authentication is enabled)
- `X-Amz-Date` - Timestamp for AWS signature (when authentication is enabled)

All other headers (such as `User-Agent`, `X-Amz-Sdk-Invocation-Id`, `amz-sdk-request`, `X-Amz-Content-Sha256`) are removed.

#### Custom headers whitelist

You can provide your own custom headers whitelist if needed:

```java
import java.util.Arrays;

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withOptimizeHeaders(true)
    .withHeadersWhitelist(Arrays.asList(
        "Host", "X-Amz-Target", "Content-Type", "Content-Length",
        "Authorization", "X-Amz-Date", "X-Custom-Header"))
    .build();
```

**Important:** When using a custom whitelist, make sure to include all headers required for
authentication (`Authorization`, `X-Amz-Date`) and operation (`Host`, `X-Amz-Target`,
`Content-Type`, `Content-Length`).

#### Combining with compression

Headers optimization can be combined with request compression for maximum bandwidth savings:

```java
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withOptimizeHeaders(true)
    .build();
```

#### No authentication mode

When connecting to Alternator clusters with authentication disabled, you can skip
authentication by simply not setting a `credentialsProvider()`. When using
`AlternatorDynamoDbClient` or `AlternatorDynamoDbAsyncClient` builders, authentication
is automatically detected based on whether `credentialsProvider()` was called. If no
credentials provider is set, the client automatically uses anonymous credentials:

```java
// No credentials provider = no authentication
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8000"))
    .build();
```

When authentication is disabled:
- The client uses anonymous credentials (no AWS signature)
- When header optimization is enabled, authentication headers (`Authorization`, `X-Amz-Date`)
  are automatically excluded from the whitelist

### Routing Scope (Datacenter/Rack Targeting)

The library supports targeting specific datacenters and racks with hierarchical fallback chains.
This allows you to prefer local nodes while gracefully falling back to other nodes when needed.

#### Basic Usage

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;

// Target a specific rack with fallback to datacenter, then cluster
RoutingScope scope = RackScope.of("dc1", "rack1",
    DatacenterScope.of("dc1",
        ClusterScope.create()));

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withRoutingScope(scope)
    .build();
```

#### Scope Types

- **ClusterScope** - Use all nodes in the cluster (no filtering)
- **DatacenterScope** - Use only nodes in a specific datacenter
- **RackScope** - Use only nodes in a specific rack within a datacenter

#### Fallback Chains

Each scope can have a fallback scope. When no nodes are available in the current scope,
the client automatically falls back to the next scope in the chain:

```java
// Rack -> Datacenter -> Cluster fallback chain
RoutingScope scope = RackScope.of("dc1", "rack1",
    DatacenterScope.of("dc1",
        ClusterScope.create()));

// Rack -> Another Rack -> Datacenter -> Cluster
RoutingScope scope = RackScope.of("dc1", "rack1",
    RackScope.of("dc1", "rack2",
        DatacenterScope.of("dc1",
            ClusterScope.create())));

// Datacenter -> Cluster fallback
RoutingScope scope = DatacenterScope.of("dc1", ClusterScope.create());

// Strict targeting (no fallback) - fails if no nodes available
RoutingScope scope = DatacenterScope.of("dc1", null);
```

#### Combining with other features

Routing scope can be combined with other builder options:

```java
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://192.168.1.100:8043"))
    .credentialsProvider(myCredentials)
    .withRoutingScope(DatacenterScope.of("dc1", ClusterScope.create()))
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withOptimizeHeaders(true)
    .build();
```

### TLS Session Tickets

The library supports TLS session tickets (RFC 5077) for quick TLS renegotiation when using HTTPS
connections. TLS session tickets allow clients to resume TLS sessions without performing a full
handshake, significantly reducing latency when reconnecting to Alternator nodes.

This is particularly beneficial in load-balanced scenarios where connections may be distributed
across different nodes, as it reduces the overhead of establishing new TLS connections.

#### Default behavior

TLS session caching is **enabled by default** with the following settings:
- Session cache size: 1024 sessions
- Session timeout: 24 hours (86400 seconds)

No configuration is needed to benefit from this feature when using HTTPS connections.

#### Custom configuration

You can customize TLS session cache settings via `TlsSessionCacheConfig`:

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.TlsSessionCacheConfig;

// Custom TLS session cache configuration
TlsSessionCacheConfig tlsConfig = TlsSessionCacheConfig.builder()
    .withEnabled(true)
    .withSessionCacheSize(200)       // Cache up to 200 sessions
    .withSessionTimeoutSeconds(3600) // 1 hour timeout
    .build();

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://localhost:8043"))
    .credentialsProvider(credentialsProvider)
    .withTlsSessionCacheConfig(tlsConfig)
    .build();
```

#### Disabling TLS session caching

If you need to disable TLS session caching:

```java
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://localhost:8043"))
    .credentialsProvider(credentialsProvider)
    .withTlsSessionCacheConfig(TlsSessionCacheConfig.disabled())
    .build();
```

#### Combining with other features

TLS session cache can be combined with other builder options:

```java
DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://localhost:8043"))
    .credentialsProvider(credentialsProvider)
    .withTlsSessionCacheConfig(TlsSessionCacheConfig.builder()
        .withSessionCacheSize(150)
        .withSessionTimeoutSeconds(7200)
        .build())
    .withCompressionAlgorithm(RequestCompressionAlgorithm.GZIP)
    .withOptimizeHeaders(true)
    .build();
```

#### Server-side configuration (ScyllaDB)

TLS session tickets require server-side support. In ScyllaDB, session tickets are **disabled by default**.
To enable them, add the following to your `scylla.yaml`:

```yaml
alternator_encryption_options:
    certificate: /path/to/scylla.crt
    keyfile: /path/to/scylla.key
    enable_session_tickets: true   # Enable TLS 1.3 session tickets
```

**Note:** TLS 1.3 is required for session tickets. Both the client (Java 11+) and server must support TLS 1.3.

#### When to tune TLS session cache

The default configuration works well for most use cases. Consider adjusting settings if:

- **High connection churn**: Increase `sessionCacheSize` if you have many short-lived connections
- **Security requirements**: Decrease `sessionTimeoutSeconds` for stricter session expiration
- **Memory constraints**: Decrease `sessionCacheSize` on memory-limited systems
- **Long-running connections**: Default settings are optimal; session resumption primarily
  benefits reconnection scenarios

### Key Route Affinity (LWT Optimization)

Key route affinity is an optimization for Lightweight Transactions (LWT) that use Paxos
consensus. By routing all requests with the same partition key to the same coordinator node,
it reduces Paxos round-trips and improves latency for conditional writes.

**Note:** Key route affinity only works reliably with synchronous DynamoDB clients
(`DynamoDbClient`). Do not use it with `DynamoDbAsyncClient` due to cross-thread
execution limitations.

#### Quick start

The simplest way to enable key route affinity is to pass an affinity mode directly:

```java
import com.scylladb.alternator.AlternatorDynamoDbClient;
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withKeyRouteAffinity(KeyRouteAffinity.RMW)
    .build();
```

#### Affinity modes

| Mode | Description |
|------|-------------|
| `KeyRouteAffinity.NONE` | Default — standard round-robin load balancing |
| `KeyRouteAffinity.RMW` | Optimize read-before-write operations (conditional updates/puts/deletes with `ConditionExpression`, `Expected`, or non-NONE `ReturnValues`) |
| `KeyRouteAffinity.ANY_WRITE` | Optimize single-item write operations (`PutItem`, `UpdateItem`, `DeleteItem`) |

#### Pre-configuring partition key names

By default, the library auto-discovers partition key names via `DescribeTable` on first
use of each table. To avoid this initial lookup, you can pre-configure them:

```java
import com.scylladb.alternator.keyrouting.KeyRouteAffinityConfig;

KeyRouteAffinityConfig keyAffinity = KeyRouteAffinityConfig.builder()
    .withType(KeyRouteAffinity.RMW)
    .withPkInfo("users", "user_id")
    .withPkInfo("orders", "order_id")
    .build();

DynamoDbClient client = AlternatorDynamoDbClient.builder()
    .endpointOverride(URI.create("https://127.0.0.1:8043"))
    .credentialsProvider(myCredentials)
    .withKeyRouteAffinity(keyAffinity)
    .build();
```

#### How it works

1. The `AffinityQueryPlanInterceptor` intercepts each DynamoDB request
2. For qualifying operations, it extracts the partition key value from the request
3. A deterministic hash (MurmurHash3) of the partition key selects a consistent node
4. All requests for the same partition key are routed to the same Alternator node
5. Non-qualifying operations continue to use round-robin load balancing

#### Observability: metrics and debug logging

Metrics are disabled by default. To enable them, configure the backend-agnostic
`KeyRouteAffinityMetrics` callback interface:

```java
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetrics;

KeyRouteAffinityConfig keyAffinity = KeyRouteAffinityConfig.builder()
    .withType(KeyRouteAffinity.RMW)
    .withPkInfo("users", "user_id")
    .withMetrics(new MyAffinityMetrics())
    .build();
```

When configured, the library emits callbacks for these canonical metric names:

| Metric | Description |
|--------|-------------|
| `alternator.affinity.requests.total` | Total requests processed by the affinity interceptor |
| `alternator.affinity.requests.affinity` | Requests routed with deterministic key affinity |
| `alternator.affinity.requests.roundrobin` | Requests that fell back to round-robin |
| `alternator.affinity.pk.cache.hits` | Partition-key cache hits |
| `alternator.affinity.pk.cache.misses` | Partition-key cache misses |
| `alternator.affinity.pk.discovery.triggered` | PK discovery attempts triggered |
| `alternator.affinity.pk.discovery.success` | Successful PK discoveries |
| `alternator.affinity.pk.discovery.failed` | Failed PK discoveries |
| `alternator.affinity.pk.cache.size` | Cached table→partition-key mappings |
| `alternator.affinity.pk.failed.tables` | Tables currently in discovery failure cooldown |

Recommended labels/tags:

- `table`
- `mode`
- `reason`

Common `reason` values include:

- `not_qualifying_request`
- `table_name_missing`
- `pk_not_cached`
- `pk_value_missing`
- `no_hash_key`
- `resource_not_found`
- `access_denied`
- `validation_error`
- `client_error`
- `transient_error`
- `unexpected_error`

Example bridge to Prometheus:

```java
import com.scylladb.alternator.keyrouting.KeyRouteAffinity;
import com.scylladb.alternator.keyrouting.KeyRouteAffinityMetrics;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public final class PrometheusAffinityMetrics implements KeyRouteAffinityMetrics {
  private final Counter requestsTotal =
      Counter.build()
          .name("alternator_affinity_requests_total")
          .help("Requests processed by the affinity interceptor")
          .labelNames("table", "mode")
          .register();
  private final Counter requestsAffinity =
      Counter.build()
          .name("alternator_affinity_requests_affinity")
          .help("Requests routed with key affinity")
          .labelNames("table", "mode")
          .register();
  private final Counter requestsRoundRobin =
      Counter.build()
          .name("alternator_affinity_requests_roundrobin")
          .help("Requests that fell back to round-robin")
          .labelNames("table", "mode", "reason")
          .register();
  private final Counter pkCacheHits =
      Counter.build()
          .name("alternator_affinity_pk_cache_hits")
          .help("Partition-key cache hits")
          .labelNames("table")
          .register();
  private final Counter pkCacheMisses =
      Counter.build()
          .name("alternator_affinity_pk_cache_misses")
          .help("Partition-key cache misses")
          .labelNames("table")
          .register();
  private final Counter pkDiscoveryTriggered =
      Counter.build()
          .name("alternator_affinity_pk_discovery_triggered")
          .help("Partition-key discovery attempts")
          .labelNames("table")
          .register();
  private final Counter pkDiscoverySuccess =
      Counter.build()
          .name("alternator_affinity_pk_discovery_success")
          .help("Successful partition-key discoveries")
          .labelNames("table")
          .register();
  private final Counter pkDiscoveryFailed =
      Counter.build()
          .name("alternator_affinity_pk_discovery_failed")
          .help("Failed partition-key discoveries")
          .labelNames("table", "reason")
          .register();
  private final Gauge pkCacheSize =
      Gauge.build()
          .name("alternator_affinity_pk_cache_size")
          .help("Partition-key cache size")
          .register();
  private final Gauge pkFailedTables =
      Gauge.build()
          .name("alternator_affinity_pk_failed_tables")
          .help("Tables currently in discovery failure cooldown")
          .register();

  @Override
  public void onRequest(String tableName, KeyRouteAffinity mode) {
    requestsTotal.labels(tableName, mode.name()).inc();
  }

  @Override
  public void onAffinityApplied(String tableName, KeyRouteAffinity mode) {
    requestsAffinity.labels(tableName, mode.name()).inc();
  }

  @Override
  public void onAffinitySkipped(String tableName, KeyRouteAffinity mode, String reason) {
    requestsRoundRobin.labels(tableName, mode.name(), reason).inc();
  }

  @Override
  public void onPartitionKeyCacheHit(String tableName) {
    pkCacheHits.labels(tableName).inc();
  }

  @Override
  public void onPartitionKeyCacheMiss(String tableName) {
    pkCacheMisses.labels(tableName).inc();
  }

  @Override
  public void onPartitionKeyDiscoveryTriggered(String tableName) {
    pkDiscoveryTriggered.labels(tableName).inc();
  }

  @Override
  public void onPartitionKeyDiscoverySucceeded(String tableName, String partitionKeyName) {
    pkDiscoverySuccess.labels(tableName).inc();
  }

  @Override
  public void onPartitionKeyDiscoveryFailed(String tableName, String reason) {
    pkDiscoveryFailed.labels(tableName, reason).inc();
  }

  @Override
  public void onPartitionKeyCacheSizeChanged(int size) {
    pkCacheSize.set(size);
  }

  @Override
  public void onPartitionKeyFailedTablesChanged(int count) {
    pkFailedTables.set(count);
  }
}
```

The Prometheus bridge above is an example only. This library does not depend on any specific
metrics backend.

Request-level debug logging is available at JUL `FINE` level from
`com.scylladb.alternator.queryplan.AffinityQueryPlanInterceptor`.
Partition-key discovery success/failure events are logged at `INFO` level from
`com.scylladb.alternator.keyrouting.PartitionKeyResolver`.

**Operational note:** request-level debug logs include a partition-key fingerprint derived from
the hashed routing key, not the raw key value. Enable `FINE` logging only in trusted environments.

Example log messages:

```text
Key affinity applied: table=users, pk_fingerprint=4f7c2d9e1a3b5c6d, target=https://192.168.1.5:8043
Key affinity skipped: table=users, reason=not_qualifying_request
PK discovery completed: table=orders, pk_attribute=order_id
PK discovery failed: table=unknown_table, reason=resource_not_found
```

#### When to use key route affinity

Key route affinity is recommended for:
- Conditional writes (`ConditionExpression`) on frequently updated keys
- Read-modify-write patterns that benefit from same-node coordination
- Workloads with high contention on specific partition keys

Key route affinity may not be beneficial for:
- Read-heavy workloads (reads don't use Paxos)
- Workloads with uniformly distributed writes across many keys
- Async clients (use sync clients only)
