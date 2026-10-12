//! The pacing rules on their own, with no timers and no transport.

use intersession_layer_messaging::peer_backoff::{BackoffPolicy, PeerBackoff};

const PEER: usize = 7;

fn cycles_until_free(backoff: &PeerBackoff<usize>) -> u32 {
    let mut cycles = 0;
    while backoff.is_cooling(&PEER) {
        backoff.tick();
        cycles += 1;
        assert!(cycles < 1000, "never stopped cooling");
    }
    cycles
}

#[test]
fn gaps_double_up_to_the_ceiling() {
    let backoff = PeerBackoff::new(BackoffPolicy::new(1, 6));
    let mut gaps = Vec::new();
    for _ in 0..6 {
        backoff.send_failed(PEER);
        gaps.push(cycles_until_free(&backoff));
    }
    assert_eq!(gaps, vec![1, 2, 4, 6, 6, 6]);
}

#[test]
fn only_the_first_failure_is_news() {
    let backoff = PeerBackoff::new(BackoffPolicy::new(1, 6));
    assert!(backoff.send_failed(PEER));
    assert!(!backoff.send_failed(PEER));
    assert!(!backoff.send_failed(PEER));
}

#[test]
fn recovery_forgets_the_failures() {
    let backoff = PeerBackoff::new(BackoffPolicy::new(1, 6));
    backoff.send_failed(PEER);
    backoff.send_failed(PEER);
    assert!(backoff.recovered(&PEER));
    assert!(!backoff.is_cooling(&PEER));
    assert!(!backoff.recovered(&PEER), "nothing left to forget");
    assert!(backoff.send_failed(PEER), "the next failure is news again");
    assert_eq!(
        cycles_until_free(&backoff),
        1,
        "and starts from the first gap"
    );
}

#[test]
fn absence_is_news_once_and_is_not_a_failure() {
    let backoff = PeerBackoff::new(BackoffPolicy::new(1, 6));
    assert!(backoff.found_unreachable(PEER));
    assert!(!backoff.found_unreachable(PEER));
    assert!(!backoff.is_cooling(&PEER));
    assert!(backoff.found_reachable(&PEER));
    assert!(!backoff.found_reachable(&PEER));
    assert!(
        backoff.found_unreachable(PEER),
        "absent again is news again"
    );
}

#[test]
fn being_listed_does_not_erase_send_failures() {
    let backoff = PeerBackoff::new(BackoffPolicy::new(2, 8));
    backoff.send_failed(PEER);
    backoff.found_reachable(&PEER);
    assert!(backoff.is_cooling(&PEER));
}

#[test]
fn peers_are_paced_independently() {
    let backoff = PeerBackoff::new(BackoffPolicy::new(3, 8));
    backoff.send_failed(PEER);
    assert!(!backoff.is_cooling(&(PEER + 1)));
}

#[test]
#[should_panic(expected = "ceiling")]
fn a_ceiling_below_the_first_gap_is_refused() {
    let _ = BackoffPolicy::new(4, 2);
}
