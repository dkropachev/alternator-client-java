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

import java.util.Objects;

/** Typed authentication and authorization settings for a test cluster. */
public final class ClusterSecuritySpec {
  public static final ClusterSecuritySpec DISABLED =
      new ClusterSecuritySpec(AuthenticationMode.ALLOW_ALL, AuthorizationMode.ALLOW_ALL, false);
  public static final ClusterSecuritySpec ENFORCED =
      new ClusterSecuritySpec(AuthenticationMode.PASSWORD, AuthorizationMode.CASSANDRA, true);

  private final AuthenticationMode authentication;
  private final AuthorizationMode authorization;
  private final boolean enforceAlternatorAuthorization;

  public ClusterSecuritySpec(
      AuthenticationMode authentication,
      AuthorizationMode authorization,
      boolean enforceAlternatorAuthorization) {
    this.authentication = Objects.requireNonNull(authentication, "authentication");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.enforceAlternatorAuthorization = enforceAlternatorAuthorization;
    validate();
  }

  public AuthenticationMode authentication() {
    return authentication;
  }

  public AuthorizationMode authorization() {
    return authorization;
  }

  public boolean enforceAlternatorAuthorization() {
    return enforceAlternatorAuthorization;
  }

  void validate() {
    if (authentication == AuthenticationMode.ALLOW_ALL
        && authorization != AuthorizationMode.ALLOW_ALL) {
      throw new IllegalArgumentException(
          "Allow-all authentication can only be used with allow-all authorization");
    }
    if (enforceAlternatorAuthorization
        && (authentication != AuthenticationMode.PASSWORD
            || authorization != AuthorizationMode.CASSANDRA)) {
      throw new IllegalArgumentException(
          "Alternator authorization enforcement requires password authentication and Cassandra authorization");
    }
  }
}
