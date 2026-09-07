package com.scylladb.alternator;

/** Current routing state for an Alternator node. */
public enum NodeHealthState {
  /** Node receives normal traffic. */
  ACTIVE,

  /** Node awaits direct validation or remains available as fallback traffic. */
  QUARANTINED,

  /** Node is excluded from normal routing and is probed in the background. */
  DOWN
}
