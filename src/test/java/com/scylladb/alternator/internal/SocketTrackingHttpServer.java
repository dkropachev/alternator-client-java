/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scylladb.alternator.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Minimal HTTP/1.1 server that records route and physical-connection identity for socket tests. */
final class SocketTrackingHttpServer implements AutoCloseable {
  interface Responder {
    Response respond(Request request) throws Exception;
  }

  static final class Request {
    private final String method;
    private final String target;
    private final Map<String, String> headers;
    private final byte[] body;
    private final String destinationAddress;
    private final int remotePort;
    private final long receivedAtNanos;

    private Request(
        String method,
        String target,
        Map<String, String> headers,
        byte[] body,
        String destinationAddress,
        int remotePort,
        long receivedAtNanos) {
      this.method = method;
      this.target = target;
      this.headers = headers;
      this.body = body;
      this.destinationAddress = destinationAddress;
      this.remotePort = remotePort;
      this.receivedAtNanos = receivedAtNanos;
    }

    String method() {
      return method;
    }

    String target() {
      return target;
    }

    String path() {
      if (target.startsWith("http://") || target.startsWith("https://")) {
        return URI.create(target).getRawPath();
      }
      int queryStart = target.indexOf('?');
      return queryStart >= 0 ? target.substring(0, queryStart) : target;
    }

    String header(String name) {
      return headers.get(name.toLowerCase(Locale.ROOT));
    }

    byte[] body() {
      return body.clone();
    }

    String destinationAddress() {
      return destinationAddress;
    }

    int remotePort() {
      return remotePort;
    }

    long receivedAtNanos() {
      return receivedAtNanos;
    }
  }

  static final class Response {
    private final int status;
    private final String reason;
    private final String contentType;
    private final byte[] body;

    private Response(int status, String reason, String contentType, byte[] body) {
      this.status = status;
      this.reason = reason;
      this.contentType = contentType;
      this.body = body;
    }

    static Response json(String body) {
      return new Response(
          200, "OK", "application/x-amz-json-1.0", body.getBytes(StandardCharsets.UTF_8));
    }

    static Response text(String body) {
      return new Response(200, "OK", "text/plain", body.getBytes(StandardCharsets.UTF_8));
    }

    static Response status(int status, String body) {
      return new Response(status, "Test", "text/plain", body.getBytes(StandardCharsets.UTF_8));
    }
  }

  private final InetAddress bindAddress;
  private final Responder responder;
  private final Object stateMonitor = new Object();
  private final List<Request> requests = new ArrayList<>();
  private final List<Long> acceptedConnectionTimesNanos = new ArrayList<>();
  private final Set<Socket> activeSockets = ConcurrentHashMap.newKeySet();
  private final Set<Thread> connectionThreads = ConcurrentHashMap.newKeySet();
  private final AtomicInteger acceptedConnections = new AtomicInteger();

  private volatile boolean running;
  private volatile IOException serverFailure;
  private ServerSocket serverSocket;
  private Thread acceptThread;

  SocketTrackingHttpServer(Responder responder) throws IOException {
    this(InetAddress.getByName("127.0.0.1"), responder);
  }

  SocketTrackingHttpServer(InetAddress bindAddress, Responder responder) {
    this.bindAddress = bindAddress;
    this.responder = responder;
  }

  void start() throws IOException {
    serverSocket = new ServerSocket();
    serverSocket.setReuseAddress(true);
    serverSocket.bind(new InetSocketAddress(bindAddress, 0));
    running = true;
    acceptThread = new Thread(this::acceptConnections, "socket-tracking-http-accept");
    acceptThread.setDaemon(true);
    acceptThread.start();
  }

  URI uri() {
    return URI.create("http://127.0.0.1:" + port());
  }

  int port() {
    return serverSocket.getLocalPort();
  }

  int requestCount() {
    synchronized (stateMonitor) {
      return requests.size();
    }
  }

  int requestCount(String targetPrefix) {
    int count = 0;
    synchronized (stateMonitor) {
      for (Request request : requests) {
        if (request.path().startsWith(targetPrefix)) {
          count++;
        }
      }
    }
    return count;
  }

  int requestCountByMethod(String method) {
    int count = 0;
    synchronized (stateMonitor) {
      for (Request request : requests) {
        if (method.equals(request.method())) {
          count++;
        }
      }
    }
    return count;
  }

  List<Request> requestsSince(int index) {
    synchronized (stateMonitor) {
      return new ArrayList<>(requests.subList(index, requests.size()));
    }
  }

  Set<Integer> uniqueRemotePorts() {
    Set<Integer> ports = new LinkedHashSet<>();
    synchronized (stateMonitor) {
      for (Request request : requests) {
        ports.add(request.remotePort());
      }
    }
    return ports;
  }

  int acceptedConnections() {
    return acceptedConnections.get();
  }

  int acceptedConnectionsAfter(long timestampNanos) {
    int count = 0;
    synchronized (stateMonitor) {
      for (long acceptedAtNanos : acceptedConnectionTimesNanos) {
        if (acceptedAtNanos > timestampNanos) {
          count++;
        }
      }
    }
    return count;
  }

  int requestCountByMethodAfter(String method, long timestampNanos) {
    int count = 0;
    synchronized (stateMonitor) {
      for (Request request : requests) {
        if (method.equals(request.method()) && request.receivedAtNanos() > timestampNanos) {
          count++;
        }
      }
    }
    return count;
  }

  int activeConnections() {
    return activeSockets.size();
  }

  Set<Integer> activeRemotePorts() {
    Set<Integer> ports = new LinkedHashSet<>();
    for (Socket socket : activeSockets) {
      ports.add(socket.getPort());
    }
    return ports;
  }

  boolean awaitRequestCount(int expected, long timeout, TimeUnit unit) throws InterruptedException {
    return awaitCondition(() -> requestCount() >= expected, timeout, unit);
  }

  boolean awaitRequestCount(String targetPrefix, int expected, long timeout, TimeUnit unit)
      throws InterruptedException {
    return awaitCondition(() -> requestCount(targetPrefix) >= expected, timeout, unit);
  }

  boolean awaitRequestCountByMethod(String method, int expected, long timeout, TimeUnit unit)
      throws InterruptedException {
    return awaitCondition(() -> requestCountByMethod(method) >= expected, timeout, unit);
  }

  boolean awaitActiveConnections(int expected, long timeout, TimeUnit unit)
      throws InterruptedException {
    return awaitCondition(() -> activeConnections() == expected, timeout, unit);
  }

  boolean awaitActiveConnectionsAtLeast(int expected, long timeout, TimeUnit unit)
      throws InterruptedException {
    return awaitCondition(() -> activeConnections() >= expected, timeout, unit);
  }

  void assertHealthy() throws IOException {
    if (serverFailure != null) {
      throw serverFailure;
    }
  }

  private boolean awaitCondition(BooleanSupplier condition, long timeout, TimeUnit unit)
      throws InterruptedException {
    long deadline = System.nanoTime() + unit.toNanos(timeout);
    synchronized (stateMonitor) {
      while (!condition.getAsBoolean()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          return false;
        }
        TimeUnit.NANOSECONDS.timedWait(stateMonitor, remaining);
      }
      return true;
    }
  }

  private void acceptConnections() {
    while (running) {
      try {
        Socket socket = serverSocket.accept();
        socket.setSoTimeout(30_000);
        recordAcceptedConnection(System.nanoTime());
        activeSockets.add(socket);
        signalStateChange();

        Thread thread =
            new Thread(
                () -> handleConnection(socket),
                "socket-tracking-http-connection-" + acceptedConnections.get());
        thread.setDaemon(true);
        connectionThreads.add(thread);
        thread.start();
      } catch (IOException e) {
        if (running) {
          recordFailure(e);
        }
      }
    }
  }

  private void handleConnection(Socket socket) {
    try (Socket current = socket) {
      InputStream input = current.getInputStream();
      OutputStream output = current.getOutputStream();
      while (running) {
        Request request;
        try {
          request = readRequest(current, input, output);
        } catch (SocketTimeoutException e) {
          continue;
        }
        if (request == null) {
          return;
        }
        recordRequest(request);
        Response response;
        try {
          response = responder.respond(request);
        } catch (Exception e) {
          recordFailure(new IOException("Test responder failed", e));
          return;
        }
        writeResponse(output, response);
      }
    } catch (IOException e) {
      if (running && !(e instanceof SocketException)) {
        recordFailure(e);
      }
    } finally {
      activeSockets.remove(socket);
      connectionThreads.remove(Thread.currentThread());
      signalStateChange();
    }
  }

  private Request readRequest(Socket socket, InputStream input, OutputStream output)
      throws IOException {
    String requestLine = readLine(input);
    while (requestLine != null && requestLine.isEmpty()) {
      requestLine = readLine(input);
    }
    if (requestLine == null) {
      return null;
    }
    long receivedAtNanos = System.nanoTime();

    String[] requestParts = requestLine.split(" ", 3);
    if (requestParts.length < 2) {
      throw new IOException("Malformed HTTP request line: " + requestLine);
    }

    Map<String, String> headers = new LinkedHashMap<>();
    while (true) {
      String header = readLine(input);
      if (header == null) {
        return null;
      }
      if (header.isEmpty()) {
        break;
      }
      int separator = header.indexOf(':');
      if (separator > 0) {
        headers.put(
            header.substring(0, separator).trim().toLowerCase(Locale.ROOT),
            header.substring(separator + 1).trim());
      }
    }

    if ("100-continue".equalsIgnoreCase(headers.get("expect"))) {
      output.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
      output.flush();
    }

    byte[] body;
    String transferEncoding = headers.get("transfer-encoding");
    if (transferEncoding != null && transferEncoding.toLowerCase(Locale.ROOT).contains("chunked")) {
      body = readChunkedBody(input);
    } else {
      int contentLength = Integer.parseInt(headers.getOrDefault("content-length", "0"));
      body = readBytes(input, contentLength);
    }

    return new Request(
        requestParts[0],
        requestParts[1],
        Collections.unmodifiableMap(headers),
        body,
        socket.getLocalAddress().getHostAddress(),
        socket.getPort(),
        receivedAtNanos);
  }

  private byte[] readChunkedBody(InputStream input) throws IOException {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    while (true) {
      String sizeLine = readLine(input);
      if (sizeLine == null) {
        throw new IOException("Unexpected EOF in chunked request body");
      }
      int extension = sizeLine.indexOf(';');
      String sizeValue = extension >= 0 ? sizeLine.substring(0, extension) : sizeLine;
      int size = Integer.parseInt(sizeValue.trim(), 16);
      if (size == 0) {
        while (true) {
          String trailer = readLine(input);
          if (trailer == null || trailer.isEmpty()) {
            return body.toByteArray();
          }
        }
      }
      body.write(readBytes(input, size));
      String chunkTerminator = readLine(input);
      if (chunkTerminator == null || !chunkTerminator.isEmpty()) {
        throw new IOException("Malformed chunked request body");
      }
    }
  }

  private byte[] readBytes(InputStream input, int length) throws IOException {
    byte[] bytes = new byte[length];
    int offset = 0;
    while (offset < length) {
      int read = input.read(bytes, offset, length - offset);
      if (read == -1) {
        throw new IOException("Unexpected EOF in HTTP request body");
      }
      offset += read;
    }
    return bytes;
  }

  private String readLine(InputStream input) throws IOException {
    StringBuilder line = new StringBuilder();
    while (true) {
      int value = input.read();
      if (value == -1) {
        return line.length() == 0 ? null : line.toString();
      }
      if (value == '\r') {
        continue;
      }
      if (value == '\n') {
        return line.toString();
      }
      line.append((char) value);
    }
  }

  private void writeResponse(OutputStream output, Response response) throws IOException {
    String headers =
        "HTTP/1.1 "
            + response.status
            + " "
            + response.reason
            + "\r\nContent-Type: "
            + response.contentType
            + "\r\nContent-Length: "
            + response.body.length
            + "\r\nConnection: keep-alive\r\n\r\n";
    output.write(headers.getBytes(StandardCharsets.US_ASCII));
    output.write(response.body);
    output.flush();
  }

  private void recordRequest(Request request) {
    synchronized (stateMonitor) {
      requests.add(request);
      stateMonitor.notifyAll();
    }
  }

  private void recordAcceptedConnection(long acceptedAtNanos) {
    synchronized (stateMonitor) {
      acceptedConnections.incrementAndGet();
      acceptedConnectionTimesNanos.add(acceptedAtNanos);
      stateMonitor.notifyAll();
    }
  }

  private void recordFailure(IOException failure) {
    if (serverFailure == null) {
      serverFailure = failure;
    }
    signalStateChange();
  }

  private void signalStateChange() {
    synchronized (stateMonitor) {
      stateMonitor.notifyAll();
    }
  }

  @Override
  public void close() throws Exception {
    running = false;
    if (serverSocket != null) {
      serverSocket.close();
    }
    for (Socket socket : new ArrayList<>(activeSockets)) {
      socket.close();
    }
    if (acceptThread != null) {
      acceptThread.join(2_000);
    }
    for (Thread thread : new ArrayList<>(connectionThreads)) {
      thread.join(2_000);
    }
  }
}
