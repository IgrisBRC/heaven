// src/tests/soul_test.rs
//
// Unit tests for every public method on Soul.
// Soul is pure data — no channels, no I/O — so these run instantly with
// `cargo test` and require no running server.
//
// Each test constructs a fresh Soul, exercises one method, and asserts on the
// return value.  Expiry is tested by passing a `now` value that is in the
// future relative to the stored expiry, which simulates the key having expired.

use crate::temple::soul::{Soul, Value};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn soul() -> Soul {
    Soul::new()
}

/// A `now` value that will never cause expiry in tests that don't want it.
const NOW: u64 = 1_000_000;

/// A `now` value far enough in the future that any key set with a short expiry
/// will appear expired.
const EXPIRED: u64 = u64::MAX;

fn str_key(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn str_val(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ── GET / SET ─────────────────────────────────────────────────────────────────

#[test]
fn get_missing_key_returns_none() {
    let mut s = soul();
    assert_eq!(s.get(str_key("missing"), NOW).unwrap(), None);
}

#[test]
fn set_then_get_returns_value() {
    let mut s = soul();
    s.set(str_key("k"), (Value::String(str_val("hello")), None));
    assert_eq!(s.get(str_key("k"), NOW).unwrap(), Some(str_val("hello")));
}

#[test]
fn get_wrong_type_returns_error() {
    let mut s = soul();
    s.set(
        str_key("k"),
        (Value::List(std::collections::VecDeque::new()), None),
    );
    assert!(s.get(str_key("k"), NOW).is_err());
}

#[test]
fn get_expired_key_returns_none() {
    let mut s = soul();
    // expires at NOW + 10, so querying at EXPIRED looks expired
    s.set(str_key("k"), (Value::String(str_val("v")), Some(NOW + 10)));
    assert_eq!(s.get(str_key("k"), EXPIRED).unwrap(), None);
}

// ── APPEND ────────────────────────────────────────────────────────────────────

#[test]
fn append_to_missing_key_creates_it() {
    let mut s = soul();
    let len = s.append(str_key("k"), str_val("hello"), NOW).unwrap();
    assert_eq!(len, 5);
    assert_eq!(s.get(str_key("k"), NOW).unwrap(), Some(str_val("hello")));
}

#[test]
fn append_to_existing_key_concatenates() {
    let mut s = soul();
    s.set(str_key("k"), (Value::String(str_val("hello")), None));
    let len = s.append(str_key("k"), str_val(" world"), NOW).unwrap();
    assert_eq!(len, 11);
    assert_eq!(
        s.get(str_key("k"), NOW).unwrap(),
        Some(str_val("hello world"))
    );
}

#[test]
fn append_wrong_type_returns_error() {
    let mut s = soul();
    s.set(
        str_key("k"),
        (Value::List(std::collections::VecDeque::new()), None),
    );
    assert!(s.append(str_key("k"), str_val("x"), NOW).is_err());
}

// ── INCR / DECR ───────────────────────────────────────────────────────────────

#[test]
fn incr_missing_key_starts_at_one() {
    let mut s = soul();
    assert_eq!(s.incr(str_key("n"), NOW).unwrap(), 1);
}

#[test]
fn incr_existing_integer() {
    let mut s = soul();
    s.set(str_key("n"), (Value::String(str_val("41")), None));
    assert_eq!(s.incr(str_key("n"), NOW).unwrap(), 42);
}

#[test]
fn incr_non_integer_returns_error() {
    let mut s = soul();
    s.set(str_key("n"), (Value::String(str_val("notanumber")), None));
    assert!(s.incr(str_key("n"), NOW).is_err());
}

#[test]
fn decr_missing_key_starts_at_minus_one() {
    let mut s = soul();
    assert_eq!(s.decr(str_key("n"), NOW).unwrap(), -1);
}

#[test]
fn decr_existing_integer() {
    let mut s = soul();
    s.set(str_key("n"), (Value::String(str_val("10")), None));
    assert_eq!(s.decr(str_key("n"), NOW).unwrap(), 9);
}

#[test]
fn decr_non_integer_returns_error() {
    let mut s = soul();
    s.set(str_key("n"), (Value::String(str_val("nan")), None));
    assert!(s.decr(str_key("n"), NOW).is_err());
}

// ── STRLEN ────────────────────────────────────────────────────────────────────

#[test]
fn strlen_missing_key_is_zero() {
    let mut s = soul();
    assert_eq!(s.strlen(str_key("k"), NOW).unwrap(), 0);
}

#[test]
fn strlen_existing_string() {
    let mut s = soul();
    s.set(str_key("k"), (Value::String(str_val("hello")), None));
    assert_eq!(s.strlen(str_key("k"), NOW).unwrap(), 5);
}

#[test]
fn strlen_wrong_type_returns_error() {
    let mut s = soul();
    s.set(
        str_key("k"),
        (Value::List(std::collections::VecDeque::new()), None),
    );
    assert!(s.strlen(str_key("k"), NOW).is_err());
}

// ── DEL ───────────────────────────────────────────────────────────────────────

#[test]
fn del_existing_keys_returns_count() {
    let mut s = soul();
    s.set(str_key("a"), (Value::String(str_val("1")), None));
    s.set(str_key("b"), (Value::String(str_val("2")), None));
    assert_eq!(
        s.del(vec![str_key("a"), str_key("b"), str_key("missing")], NOW),
        2
    );
}

#[test]
fn del_removes_key() {
    let mut s = soul();
    s.set(str_key("k"), (Value::String(str_val("v")), None));
    s.del(vec![str_key("k")], NOW);
    assert_eq!(s.get(str_key("k"), NOW).unwrap(), None);
}

// ── EXISTS ────────────────────────────────────────────────────────────────────

#[test]
fn exists_returns_count_of_present_keys() {
    let mut s = soul();
    s.set(str_key("a"), (Value::String(str_val("1")), None));
    assert_eq!(s.exists(vec![str_key("a"), str_key("missing")], NOW), 1);
}

#[test]
fn exists_counts_duplicate_keys_multiple_times() {
    let mut s = soul();
    s.set(str_key("a"), (Value::String(str_val("1")), None));
    // Redis spec: EXISTS a a returns 2
    assert_eq!(s.exists(vec![str_key("a"), str_key("a")], NOW), 2);
}

// ── HSET / HGET / HDEL / HEXISTS / HLEN / HMGET / HGETALL ───────────────────

#[test]
fn hset_creates_fields_and_returns_added_count() {
    let mut s = soul();
    let pairs = vec![
        (str_val("f1"), str_val("v1")),
        (str_val("f2"), str_val("v2")),
    ];
    assert_eq!(s.hset(str_key("h"), pairs, NOW).unwrap(), 2);
}

#[test]
fn hset_update_existing_field_does_not_increment_count() {
    let mut s = soul();
    s.hset(str_key("h"), vec![(str_val("f"), str_val("v1"))], NOW)
        .unwrap();
    let added = s
        .hset(str_key("h"), vec![(str_val("f"), str_val("v2"))], NOW)
        .unwrap();
    assert_eq!(added, 0);
    assert_eq!(
        s.hget(str_key("h"), str_val("f"), NOW).unwrap(),
        Some(str_val("v2"))
    );
}

#[test]
fn hget_missing_field_returns_none() {
    let mut s = soul();
    s.hset(str_key("h"), vec![(str_val("f"), str_val("v"))], NOW)
        .unwrap();
    assert_eq!(s.hget(str_key("h"), str_val("nope"), NOW).unwrap(), None);
}

#[test]
fn hget_missing_key_returns_none() {
    let mut s = soul();
    assert_eq!(s.hget(str_key("nope"), str_val("f"), NOW).unwrap(), None);
}

#[test]
fn hdel_removes_fields_and_returns_count() {
    let mut s = soul();
    s.hset(
        str_key("h"),
        vec![
            (str_val("f1"), str_val("v1")),
            (str_val("f2"), str_val("v2")),
        ],
        NOW,
    )
    .unwrap();
    assert_eq!(
        s.hdel(str_key("h"), vec![str_val("f1"), str_val("missing")], NOW)
            .unwrap(),
        1
    );
    assert_eq!(s.hget(str_key("h"), str_val("f1"), NOW).unwrap(), None);
}

#[test]
fn hexists_present_field_returns_one() {
    let mut s = soul();
    s.hset(str_key("h"), vec![(str_val("f"), str_val("v"))], NOW)
        .unwrap();
    assert_eq!(s.hexists(str_key("h"), str_val("f"), NOW).unwrap(), 1);
}

#[test]
fn hexists_missing_field_returns_zero() {
    let mut s = soul();
    s.hset(str_key("h"), vec![(str_val("f"), str_val("v"))], NOW)
        .unwrap();
    assert_eq!(s.hexists(str_key("h"), str_val("nope"), NOW).unwrap(), 0);
}

#[test]
fn hlen_returns_number_of_fields() {
    let mut s = soul();
    s.hset(
        str_key("h"),
        vec![
            (str_val("f1"), str_val("v1")),
            (str_val("f2"), str_val("v2")),
            (str_val("f3"), str_val("v3")),
        ],
        NOW,
    )
    .unwrap();
    assert_eq!(s.hlen(str_key("h"), NOW).unwrap(), 3);
}

#[test]
fn hmget_returns_values_in_order_with_nones_for_missing() {
    let mut s = soul();
    s.hset(str_key("h"), vec![(str_val("f1"), str_val("v1"))], NOW)
        .unwrap();
    let result = s
        .hmget(str_key("h"), vec![str_val("f1"), str_val("f2")], NOW)
        .unwrap();
    assert_eq!(result, Some(vec![Some(str_val("v1")), None]));
}

#[test]
fn hgetall_returns_flat_interleaved_field_value_list() {
    let mut s = soul();
    s.hset(
        str_key("h"),
        vec![
            (str_val("name"), str_val("alice")),
            (str_val("age"), str_val("30")),
        ],
        NOW,
    )
    .unwrap();

    // Soul returns a flat Vec: [field, value, field, value, ...]
    // This mirrors the RESP2 wire format — pairs are NOT tuples (that's RESP3).
    let flat = s.hgetall(str_key("h"), NOW).unwrap().unwrap();

    // Must be even length: one value per field
    assert_eq!(
        flat.len(),
        4,
        "Expected 4 elements (2 fields × 2), got: {:?}",
        flat
    );

    // Collect into a HashMap so we can assert without caring about HashMap order
    let map: std::collections::HashMap<Vec<u8>, Vec<u8>> = flat
        .chunks(2)
        .map(|chunk| {
            let field = chunk[0].clone().unwrap();
            let value = chunk[1].clone().unwrap();
            (field, value)
        })
        .collect();

    assert_eq!(map.get(&str_val("name")), Some(&str_val("alice")));
    assert_eq!(map.get(&str_val("age")), Some(&str_val("30")));
}

// ── LPUSH / RPUSH / LPOP / RPOP / LLEN / LRANGE / LINDEX / LSET / LREM ──────

#[test]
fn lpush_creates_list_and_prepends() {
    let mut s = soul();
    // lpush a b → list is [b, a]
    assert_eq!(
        s.lpush(str_key("l"), vec![str_val("a"), str_val("b")], NOW)
            .unwrap(),
        2
    );
    let range = s.lrange(str_key("l"), 0, -1, NOW).unwrap().unwrap();
    assert_eq!(range[0], Some(str_val("b")));
    assert_eq!(range[1], Some(str_val("a")));
}

#[test]
fn rpush_appends_in_order() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a"), str_val("b")], NOW)
        .unwrap();
    let range = s.lrange(str_key("l"), 0, -1, NOW).unwrap().unwrap();
    assert_eq!(range, vec![Some(str_val("a")), Some(str_val("b"))]);
}

#[test]
fn lpop_removes_and_returns_head() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a"), str_val("b")], NOW)
        .unwrap();
    assert_eq!(s.lpop(str_key("l"), NOW).unwrap(), Some(str_val("a")));
    assert_eq!(s.llen(str_key("l"), NOW).unwrap(), 1);
}

#[test]
fn rpop_removes_and_returns_tail() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a"), str_val("b")], NOW)
        .unwrap();
    assert_eq!(s.rpop(str_key("l"), NOW).unwrap(), Some(str_val("b")));
}

#[test]
fn lpop_empty_list_key_deleted() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("only")], NOW).unwrap();
    s.lpop(str_key("l"), NOW).unwrap();
    assert_eq!(s.llen(str_key("l"), NOW).unwrap(), 0);
}

#[test]
fn lpop_missing_key_returns_none() {
    let mut s = soul();
    assert_eq!(s.lpop(str_key("nope"), NOW).unwrap(), None);
}

#[test]
fn llen_missing_key_is_zero() {
    let mut s = soul();
    assert_eq!(s.llen(str_key("l"), NOW).unwrap(), 0);
}

#[test]
fn lrange_full_range() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    let r = s.lrange(str_key("l"), 0, -1, NOW).unwrap().unwrap();
    assert_eq!(
        r,
        vec![Some(str_val("a")), Some(str_val("b")), Some(str_val("c"))]
    );
}

#[test]
fn lrange_negative_indices() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    let r = s.lrange(str_key("l"), -2, -1, NOW).unwrap().unwrap();
    assert_eq!(r, vec![Some(str_val("b")), Some(str_val("c"))]);
}

#[test]
fn lrange_out_of_bounds_returns_empty() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a")], NOW).unwrap();
    let r = s.lrange(str_key("l"), 5, 10, NOW).unwrap();
    // Either None or Some([]) depending on impl — both are valid, just not panic
    assert!(r.is_none() || r.unwrap().is_empty());
}

#[test]
fn lindex_valid_index() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    assert_eq!(s.lindex(str_key("l"), 1, NOW).unwrap(), Some(str_val("b")));
}

#[test]
fn lindex_negative_index() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    assert_eq!(s.lindex(str_key("l"), -1, NOW).unwrap(), Some(str_val("c")));
}

#[test]
fn lindex_out_of_range_returns_none() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a")], NOW).unwrap();
    assert_eq!(s.lindex(str_key("l"), 99, NOW).unwrap(), None);
}

#[test]
fn lset_replaces_element() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a"), str_val("b")], NOW)
        .unwrap();
    s.lset(str_key("l"), 1, str_val("X"), NOW).unwrap();
    assert_eq!(s.lindex(str_key("l"), 1, NOW).unwrap(), Some(str_val("X")));
}

#[test]
fn lset_out_of_range_returns_error() {
    let mut s = soul();
    s.rpush(str_key("l"), vec![str_val("a")], NOW).unwrap();
    assert!(s.lset(str_key("l"), 99, str_val("X"), NOW).is_err());
}

#[test]
fn lrem_removes_from_head() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![
            str_val("a"),
            str_val("b"),
            str_val("a"),
            str_val("c"),
            str_val("a"),
        ],
        NOW,
    )
    .unwrap();
    // count=2 removes first 2 "a"s
    let removed = s.lrem(str_key("l"), 2, str_val("a"), NOW).unwrap();
    assert_eq!(removed, 2);
    let r = s.lrange(str_key("l"), 0, -1, NOW).unwrap().unwrap();
    assert_eq!(
        r,
        vec![Some(str_val("b")), Some(str_val("c")), Some(str_val("a"))]
    );
}

#[test]
fn lrem_count_zero_removes_all() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("a")],
        NOW,
    )
    .unwrap();
    let removed = s.lrem(str_key("l"), 0, str_val("a"), NOW).unwrap();
    assert_eq!(removed, 2);
}

#[test]
fn lrem_negative_count_removes_from_tail() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![
            str_val("a"),
            str_val("b"),
            str_val("a"),
            str_val("c"),
            str_val("a"),
        ],
        NOW,
    )
    .unwrap();
    let removed = s.lrem(str_key("l"), -2, str_val("a"), NOW).unwrap();
    assert_eq!(removed, 2);
    let r = s.lrange(str_key("l"), 0, -1, NOW).unwrap().unwrap();
    assert_eq!(
        r,
        vec![Some(str_val("a")), Some(str_val("b")), Some(str_val("c"))]
    );
}

// ── LPOP_M / RPOP_M ──────────────────────────────────────────────────────────

#[test]
fn lpop_m_pops_n_elements() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    let popped = s.lpop_m(str_key("l"), 2, NOW).unwrap().unwrap();
    assert_eq!(popped, vec![Some(str_val("a")), Some(str_val("b"))]);
    assert_eq!(s.llen(str_key("l"), NOW).unwrap(), 1);
}

#[test]
fn rpop_m_pops_n_elements_from_tail() {
    let mut s = soul();
    s.rpush(
        str_key("l"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    let popped = s.rpop_m(str_key("l"), 2, NOW).unwrap().unwrap();
    assert_eq!(popped, vec![Some(str_val("c")), Some(str_val("b"))]);
}

// ── SADD / SREM / SISMEMBER / SMEMBERS ───────────────────────────────────────

#[test]
fn sadd_adds_new_members_returns_count() {
    let mut s = soul();
    assert_eq!(
        s.sadd(str_key("s"), vec![str_val("a"), str_val("b")], NOW)
            .unwrap(),
        2
    );
}

#[test]
fn sadd_duplicate_not_counted() {
    let mut s = soul();
    s.sadd(str_key("s"), vec![str_val("a")], NOW).unwrap();
    assert_eq!(
        s.sadd(str_key("s"), vec![str_val("a"), str_val("b")], NOW)
            .unwrap(),
        1
    );
}

#[test]
fn srem_removes_members_returns_count() {
    let mut s = soul();
    s.sadd(
        str_key("s"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    assert_eq!(
        s.srem(str_key("s"), vec![str_val("a"), str_val("missing")], NOW)
            .unwrap(),
        1
    );
    assert_eq!(s.sismember(str_key("s"), str_val("a"), NOW).unwrap(), 0);
}

#[test]
fn sismember_present_returns_one() {
    let mut s = soul();
    s.sadd(str_key("s"), vec![str_val("x")], NOW).unwrap();
    assert_eq!(s.sismember(str_key("s"), str_val("x"), NOW).unwrap(), 1);
}

#[test]
fn sismember_absent_returns_zero() {
    let mut s = soul();
    assert_eq!(s.sismember(str_key("s"), str_val("nope"), NOW).unwrap(), 0);
}

#[test]
fn smembers_returns_all_members() {
    let mut s = soul();
    s.sadd(
        str_key("s"),
        vec![str_val("a"), str_val("b"), str_val("c")],
        NOW,
    )
    .unwrap();
    let mut members = s.smembers(str_key("s"), NOW).unwrap().unwrap();
    members.sort();
    assert_eq!(
        members,
        vec![Some(str_val("a")), Some(str_val("b")), Some(str_val("c"))]
    );
}

// ── EXPIRE / TTL ──────────────────────────────────────────────────────────────

#[test]
fn expire_sets_expiry_on_key() {
    let mut s = soul();
    s.set(str_key("k"), (Value::String(str_val("v")), None));
    // expire at NOW + 100 seconds
    s.expire(str_key("k"), NOW + 100, NOW);
    // still accessible at NOW
    assert!(s.get(str_key("k"), NOW).unwrap().is_some());
    // gone at NOW + 200
    assert!(s.get(str_key("k"), NOW + 200).unwrap().is_none());
}

#[test]
fn expire_missing_key_returns_zero() {
    let mut s = soul();
    assert_eq!(s.expire(str_key("nope"), NOW + 10, NOW), 0);
}

#[test]
fn ttl_returns_remaining_seconds() {
    use std::time::{Duration, SystemTime};
    let mut s = soul();
    s.set(str_key("k"), (Value::String(str_val("v")), Some(NOW + 100)));
    // TTL takes a SystemTime; we use UNIX_EPOCH + NOW as the reference point
    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(NOW);
    let ttl = s.ttl(str_key("k"), at);
    // Should be ~100 seconds remaining
    assert!(ttl > 0 && ttl <= 100);
}

#[test]
fn ttl_missing_key_returns_minus_two() {
    use std::time::SystemTime;
    let mut s = soul();
    assert_eq!(s.ttl(str_key("nope"), SystemTime::now()), -2);
}
