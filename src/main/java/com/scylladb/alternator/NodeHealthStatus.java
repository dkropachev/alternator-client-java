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

/** Snapshot of one node's health state. */
public final class NodeHealthStatus {
  private final NodeHealthState state;
  private final int consecutiveFailures;
  private final int consecutiveSuccesses;
  private final long updatedAtNanos;
  private final long generation;

  /**
   * Creates a node health status snapshot.
   *
   * @param state current node state
   * @param consecutiveFailures consecutive DynamoDB traffic failures in the current routing state
   * @param consecutiveSuccesses consecutive successful observations in the current recovery state
   * @param updatedAtNanos monotonic timestamp from {@link System#nanoTime()}
   */
  public NodeHealthStatus(
      NodeHealthState state,
      int consecutiveFailures,
      int consecutiveSuccesses,
      long updatedAtNanos) {
    this(state, consecutiveFailures, consecutiveSuccesses, updatedAtNanos, 0);
  }

  /**
   * Creates a node health status snapshot with an attempt-generation token.
   *
   * @param state current node state
   * @param consecutiveFailures consecutive DynamoDB traffic failures in the current routing state
   * @param consecutiveSuccesses consecutive successful observations in the current recovery state
   * @param updatedAtNanos monotonic timestamp from {@link System#nanoTime()}
   * @param generation token incremented whenever the node enters {@link NodeHealthState#DOWN}
   */
  public NodeHealthStatus(
      NodeHealthState state,
      int consecutiveFailures,
      int consecutiveSuccesses,
      long updatedAtNanos,
      long generation) {
    this.state = state;
    this.consecutiveFailures = consecutiveFailures;
    this.consecutiveSuccesses = consecutiveSuccesses;
    this.updatedAtNanos = updatedAtNanos;
    this.generation = generation;
  }

  /**
   * Returns the node state.
   *
   * @return the node state
   */
  public NodeHealthState getState() {
    return state;
  }

  /**
   * Returns consecutive DynamoDB traffic failures in the current routing state.
   *
   * @return consecutive DynamoDB traffic failures
   */
  public int getConsecutiveFailures() {
    return consecutiveFailures;
  }

  /**
   * Returns consecutive successful observations in the current recovery state.
   *
   * @return consecutive successful observations in the current recovery state
   */
  public int getConsecutiveSuccesses() {
    return consecutiveSuccesses;
  }

  /**
   * Returns the monotonic update timestamp.
   *
   * @return timestamp from {@link System#nanoTime()}
   */
  public long getUpdatedAtNanos() {
    return updatedAtNanos;
  }

  /**
   * Returns the current attempt-generation token.
   *
   * <p>Traffic attempts capture this value when they are routed. A result from an older generation
   * is ignored after the node has entered down state and begun a later recovery cycle.
   *
   * @return current node health generation
   */
  public long getGeneration() {
    return generation;
  }
}
