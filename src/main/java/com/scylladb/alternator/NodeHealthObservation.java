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
package com.scylladb.alternator;

/** Result observed for an Alternator node health decision. */
public enum NodeHealthObservation {
  /** A routed DynamoDB request produced a response considered healthy for routing purposes. */
  TRAFFIC_SUCCESS,

  /** A routed DynamoDB request failed before receiving an HTTP response. */
  TRAFFIC_FAILURE,

  /**
   * A direct node-health probe completed successfully; activates quarantine or advances down-node
   * recovery.
   */
  PROBE_SUCCESS,

  /**
   * A direct node-health probe failed; leaves quarantine unchanged or resets down-node recovery.
   */
  PROBE_FAILURE
}
