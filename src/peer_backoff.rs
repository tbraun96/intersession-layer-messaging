//! Retry pacing for peers the outbound loop cannot currently reach.
//!
//! The loop wakes every `OUTBOUND_POLL` and used to try every queued peer on
//! every wake. A peer that is gone, or whose stream died while it was still
//! listed, was therefore retried five times a second forever, and each attempt
//! logged. This spaces the attempts out and makes the transition the only thing
//! worth logging.
//!
//! Time is counted in poll cycles, not read from a clock: `Instant::now` does
//! not exist on wasm32, and the loop already ticks at a fixed rate.
//!
//! Nothing here touches the queue. A message to a cooling peer stays stored and
//! is sent when the cooldown ends, the peer is heard from, or it reconnects.

use dashmap::DashMap;
use std::hash::Hash;

/// Gaps between attempts, in poll cycles. There is no `Default`: the owner of
/// the loop states the pacing it wants.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BackoffPolicy {
    first_gap_cycles: u32,
    max_gap_cycles: u32,
}

impl BackoffPolicy {
    /// The gap after the first failure, doubling per further failure up to
    /// `max_gap_cycles`. Evaluated at compile time when used in a `const`.
    pub const fn new(first_gap_cycles: u32, max_gap_cycles: u32) -> Self {
        assert!(first_gap_cycles >= 1, "a gap of zero cycles is no backoff");
        assert!(
            max_gap_cycles >= first_gap_cycles,
            "the ceiling is below the first gap"
        );
        Self {
            first_gap_cycles,
            max_gap_cycles,
        }
    }

    fn gap_after(&self, failures: u32) -> u32 {
        let doublings = failures.saturating_sub(1).min(31);
        self.first_gap_cycles
            .saturating_mul(1u32 << doublings)
            .min(self.max_gap_cycles)
    }
}

#[derive(Default)]
struct PeerState {
    failures: u32,
    cooldown: u32,
    unreachable: bool,
}

pub struct PeerBackoff<P: Hash + Eq + Copy> {
    policy: BackoffPolicy,
    peers: DashMap<P, PeerState>,
}

impl<P: Hash + Eq + Copy> PeerBackoff<P> {
    pub fn new(policy: BackoffPolicy) -> Self {
        Self {
            policy,
            peers: DashMap::new(),
        }
    }

    /// One poll cycle has passed.
    pub fn tick(&self) {
        for mut state in self.peers.iter_mut() {
            state.cooldown = state.cooldown.saturating_sub(1);
        }
    }

    pub fn is_cooling(&self, peer: &P) -> bool {
        self.peers.get(peer).is_some_and(|state| state.cooldown > 0)
    }

    /// A send to `peer` failed. True only for the first failure since the peer
    /// last worked, which is the one worth announcing.
    pub fn send_failed(&self, peer: P) -> bool {
        let mut state = self.peers.entry(peer).or_default();
        state.failures = state.failures.saturating_add(1);
        state.cooldown = self.policy.gap_after(state.failures);
        state.failures == 1
    }

    /// `peer` is not in the transport's connected set. True only on the
    /// transition into absent. No cooldown: noticing absence costs nothing.
    pub fn found_unreachable(&self, peer: P) -> bool {
        let mut state = self.peers.entry(peer).or_default();
        !std::mem::replace(&mut state.unreachable, true)
    }

    /// `peer` is in the connected set. True if it was last found absent. Leaves
    /// send failures alone: a peer can be listed with a stream that is dead.
    pub fn found_reachable(&self, peer: &P) -> bool {
        self.peers
            .get_mut(peer)
            .is_some_and(|mut state| std::mem::replace(&mut state.unreachable, false))
    }

    /// `peer` has just been heard from or sent to successfully, or has
    /// reconnected.
    /// Everything about its past failures is forgotten. True if it had any.
    pub fn recovered(&self, peer: &P) -> bool {
        self.peers.remove(peer).is_some()
    }
}
