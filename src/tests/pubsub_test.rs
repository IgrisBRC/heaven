// src/tests/pubsub_test.rs
//
// Integration tests for SUBSCRIBE / UNSUBSCRIBE / PUBLISH.
// Requires a running heaven server on 127.0.0.1:6379.
//
// All wire I/O is Vec<u8> / &[u8] — no UTF-8 assumption anywhere.
//
// Pub/Sub requires two concurrent connections:
//   - subscriber thread: sends SUBSCRIBE and reads push messages
//   - publisher thread: sends PUBLISH commands
//
// We synchronise them with a Barrier so the subscriber is confirmed
// subscribed before the publisher fires.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

// ── Connection ────────────────────────────────────────────────────────────────

fn connect() -> TcpStream {
    let s = TcpStream::connect("127.0.0.1:6379")
        .expect("Could not connect to heaven. Is the server running on :6379?");
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s
}

// ── RESP2 serialiser ──────────────────────────────────────────────────────────

fn send_command(stream: &mut TcpStream, args: &[&[u8]]) {
    let mut packet = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        let mut header = format!("${}\r\n", arg.len()).into_bytes();
        packet.append(&mut header);
        packet.extend_from_slice(arg);
        packet.extend_from_slice(b"\r\n");
    }
    stream.write_all(&packet).unwrap();
}

fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or(0);
    buf.truncate(n);
    buf
}

// ── RESP2 helpers ─────────────────────────────────────────────────────────────

/// Count non-overlapping occurrences of `needle` in `haystack`.
fn count_subsequence(haystack: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut i = 0;
    while i + needle.len() <= haystack.len() {
        if &haystack[i..i + needle.len()] == needle {
            count += 1;
            i += needle.len();
        } else {
            i += 1;
        }
    }
    count
}

/// Returns true if `haystack` contains `needle` as a contiguous subsequence.
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    count_subsequence(haystack, needle) > 0
}

/// Parse a RESP2 integer response like b":3\r\n" -> 3.
fn parse_integer(resp: &[u8]) -> i64 {
    assert_eq!(resp[0], b':', "Expected integer response, got: {:?}", resp);
    let end = resp.iter().position(|&b| b == b'\r').unwrap_or(resp.len());
    std::str::from_utf8(&resp[1..end]).unwrap().parse().unwrap()
}

macro_rules! b {
    ($s:expr) => { $s.as_bytes() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Subscribe to one channel, publish one message, verify the push arrives.
#[test]
fn test_subscribe_receives_published_message() {
    let channel: &[u8] = b"pubsub:test:basic";
    let payload: &[u8] = b"hello from publisher";

    let barrier = Arc::new(Barrier::new(2));
    let barrier_sub = Arc::clone(&barrier);

    let channel_sub = channel.to_vec();
    let payload_sub = payload.to_vec();

    let subscriber = thread::spawn(move || {
        let mut s = connect();

        send_command(&mut s, &[b!("SUBSCRIBE"), &channel_sub]);

        // Read the subscribe confirmation frame:
        // *3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:1\r\n
        let confirm = read_response(&mut s);
        assert!(
            contains_bytes(&confirm, b"subscribe"),
            "Expected subscribe confirmation, got: {:?}", confirm
        );

        // Signal publisher we're ready
        barrier_sub.wait();

        // Read the push message:
        // *3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<payload>\r\n
        let msg = read_response(&mut s);
        assert!(contains_bytes(&msg, b"message"),   "Missing 'message' frame type: {:?}", msg);
        assert!(contains_bytes(&msg, &channel_sub), "Missing channel in push: {:?}", msg);
        assert!(contains_bytes(&msg, &payload_sub), "Missing payload in push: {:?}", msg);
    });

    let publisher = thread::spawn(move || {
        let mut s = connect();

        barrier.wait();
        // Small extra sleep to let the subscriber's read() call get posted
        thread::sleep(Duration::from_millis(50));

        send_command(&mut s, &[b!("PUBLISH"), channel, payload]);

        let resp = read_response(&mut s);
        assert_eq!(
            parse_integer(&resp), 1,
            "Expected 1 subscriber to receive message, got: {:?}", resp
        );
    });

    publisher.join().unwrap();
    subscriber.join().unwrap();
}

/// Subscribe to multiple channels, verify each gets its own confirmation frame.
#[test]
fn test_subscribe_multiple_channels() {
    let mut s = connect();
    send_command(&mut s, &[
        b!("SUBSCRIBE"),
        b!("pubsub:multi:a"),
        b!("pubsub:multi:b"),
        b!("pubsub:multi:c"),
    ]);

    let resp = read_response(&mut s);

    // heaven sends one *3 confirmation frame per channel
    let frame_count = count_subsequence(&resp, b"subscribe");
    assert!(
        frame_count >= 3,
        "Expected 3 subscribe confirmations, got {}: {:?}", frame_count, resp
    );
}

/// After SUBSCRIBE, sending a non-pub/sub command should return an error.
#[test]
fn test_subscriber_mode_rejects_regular_commands() {
    let mut s = connect();
    send_command(&mut s, &[b!("SUBSCRIBE"), b!("pubsub:mode:ch")]);
    read_response(&mut s); // consume confirmation

    send_command(&mut s, &[b!("SET"), b!("pubsub:mode:k"), b!("v")]);
    let resp = read_response(&mut s);
    assert_eq!(resp[0], b'-', "Expected error in subscriber mode, got: {:?}", resp);
}

/// UNSUBSCRIBE without args leaves pub/sub mode (subscriber count -> 0).
#[test]
fn test_unsubscribe_all() {
    let mut s = connect();
    send_command(&mut s, &[b!("SUBSCRIBE"), b!("pubsub:unsub:a"), b!("pubsub:unsub:b")]);
    read_response(&mut s); // consume confirmations

    send_command(&mut s, &[b!("UNSUBSCRIBE")]);
    let resp = read_response(&mut s);

    assert!(
        contains_bytes(&resp, b"unsubscribe"),
        "Expected unsubscribe frames, got: {:?}", resp
    );
    // The final frame must carry count :0
    assert!(
        contains_bytes(&resp, b":0\r\n"),
        "Expected :0 final count, got: {:?}", resp
    );
}

/// PUBLISH to a channel with no subscribers returns :0.
#[test]
fn test_publish_no_subscribers_returns_zero() {
    let mut s = connect();
    send_command(&mut s, &[b!("PUBLISH"), b!("pubsub:empty:ch"), b!("nobody home")]);
    let resp = read_response(&mut s);
    assert_eq!(parse_integer(&resp), 0, "Expected :0, got: {:?}", resp);
}

/// Multiple subscribers on the same channel each receive the message.
#[test]
fn test_multiple_subscribers_all_receive_message() {
    let channel: &[u8] = b"pubsub:fan:ch";
    let n_subscribers: usize = 3;
    let barrier = Arc::new(Barrier::new(n_subscribers + 1));

    let mut handles = vec![];

    for _ in 0..n_subscribers {
        let b = Arc::clone(&barrier);
        let ch = channel.to_vec();
        handles.push(thread::spawn(move || {
            let mut s = connect();
            send_command(&mut s, &[b!("SUBSCRIBE"), &ch]);
            read_response(&mut s); // consume confirmation

            b.wait();

            let msg = read_response(&mut s);
            assert!(
                contains_bytes(&msg, b"broadcast"),
                "Subscriber missed message: {:?}", msg
            );
        }));
    }

    barrier.wait();
    thread::sleep(Duration::from_millis(50));

    let mut pub_conn = connect();
    send_command(&mut pub_conn, &[b!("PUBLISH"), channel, b!("broadcast")]);
    let resp = read_response(&mut pub_conn);
    assert_eq!(
        parse_integer(&resp), n_subscribers as i64,
        "Expected {} receivers, got: {:?}", n_subscribers, resp
    );

    for h in handles {
        h.join().unwrap();
    }
}

/// PING is valid inside pub/sub mode.
#[test]
fn test_ping_in_subscriber_mode() {
    let mut s = connect();
    send_command(&mut s, &[b!("SUBSCRIBE"), b!("pubsub:ping:ch")]);
    read_response(&mut s); // consume subscription confirmation

    send_command(&mut s, &[b!("PING")]);
    let resp = read_response(&mut s);

    // In pub/sub mode Redis returns *2\r\n$4\r\npong\r\n$0\r\n\r\n
    assert!(
        contains_bytes(&resp, b"pong") || contains_bytes(&resp, b"PONG"),
        "Expected pong in subscriber mode, got: {:?}", resp
    );
}
