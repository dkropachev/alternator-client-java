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
package com.scylladb.alternator.testinfra;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** Connection material for one transport exposed by a test cluster. */
public final class AlternatorConnection {
  private final URI seedEndpoint;
  private final List<URI> nodeEndpoints;
  private final AwsCredentialsProvider credentials;
  private final Path caCertificatePath;

  AlternatorConnection(
      URI seedEndpoint,
      List<URI> nodeEndpoints,
      AwsCredentialsProvider credentials,
      Path caCertificatePath) {
    this.seedEndpoint = seedEndpoint;
    this.nodeEndpoints = Collections.unmodifiableList(new ArrayList<>(nodeEndpoints));
    this.credentials = credentials;
    this.caCertificatePath = caCertificatePath;
  }

  public URI seedEndpoint() {
    return seedEndpoint;
  }

  public List<URI> nodeEndpoints() {
    return nodeEndpoints;
  }

  public AwsCredentialsProvider credentials() {
    return credentials;
  }

  public Path caCertificatePath() {
    return caCertificatePath;
  }
}
