// src/tests/pubsub_test.rs
//
// Integration tests for SUBSCRIBE / UNSUBSCRIBE / PUBLISH.
// Each test starts its own heaven server on a unique port.
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
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

/// Helper function to build and return path to the test binary
fn build_binary() -> PathBuf {
    let output = Command::new("cargo")
        .args(["build", "--release"])
        .output()
        .expect("Failed to build binary");

    if !output.status.success() {
        panic!(
            "Failed to build binary: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target/release/heaven");
    path
}

/// Start the heaven server as a subprocess on a given port
fn start_server(port: u16) -> Child {
    let binary = build_binary();
    let dbfilename = format!("test_{}.rdb", port);

    Command::new(&binary)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--io-threads")
        .arg("2")
        .arg("--event-limit")
        .arg("128")
        .arg("--dbfilename")
        .arg(&dbfilename)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server")
}

/// Stop the server gracefully
fn stop_server(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Connect to the server on the given port
fn connect(port: u16) -> TcpStream {
    let mut last_err = None;
    for _ in 0..50 {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                return s;
            }
            Err(e) => {
                last_err = Some(e);
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
    panic!("Could not connect to heaven on port {}: {:?}", port, last_err);
}

/// RESP2 serialiser
fn send_command(stream: &mut TcpStream, args: &[&[u8]]) {
    let mut packet = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        packet.extend_from_slice(b"$");
        packet.extend_from_slice(arg.len().to_string().as_bytes());
        packet.extend_from_slice(b"\r\n");
        packet.extend_from_slice(arg);
        packet.extend_from_slice(b"\r\n");
    }
    stream.write_all(&packet).expect("write failed");
    stream.flush().expect("flush failed");
}

/// Read a single RESP2 response (handles bulk strings, arrays, integers, simple strings, errors)
fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 1];
    let mut response = Vec::new();

    // Read first byte to determine type
    stream.read_exact(&mut buf).expect("read first byte failed");
    response.push(buf[0]);

    match buf[0] {
        b'+' | b'-' | b':' => {
            // Simple string, error, or integer - read until \r\n
            loop {
                stream.read_exact(&mut buf).expect("read failed");
                response.push(buf[0]);
                if response.ends_with(b"\r\n") {
                    break;
                }
            }
        }
        b'$' => {
            // Bulk string - read length then data
            let mut len_buf = Vec::new();
            loop {
                stream.read_exact(&mut buf).expect("read failed");
                len_buf.push(buf[0]);
                if len_buf.ends_with(b"\r\n") {
                    break;
                }
            }
            response.extend_from_slice(&len_buf);
            let len_str = String::from_utf8_lossy(&len_buf[..len_buf.len()-2]);
            let len: usize = len_str.parse().expect("invalid bulk string length");
            if len > 0 {
                let mut data = vec![0u8; len + 2]; // +2 for \r\n
                stream.read_exact(&mut data).expect("read bulk data failed");
                response.extend_from_slice(&data);
            }
        }
        b'*' => {
            // Array - read count then each element recursively
            let mut count_buf = Vec::new();
            loop {
                stream.read_exact(&mut buf).expect("read failed");
                count_buf.push(buf[0]);
                if count_buf.ends_with(b"\r\n") {
                    break;
                }
            }
            response.extend_from_slice(&count_buf);
            let count_str = String::from_utf8_lossy(&count_buf[..count_buf.len()-2]);
            let count: usize = count_str.parse().expect("invalid array count");
            for _ in 0..count {
                let elem = read_response(stream);
                response.extend_from_slice(&elem);
            }
        }
        _ => panic!("Unknown RESP type: {}", buf[0] as char),
    }
    response
}

/// Read all available responses (for SUBSCRIBE which returns multiple concatenated frames)
fn read_all_responses(stream: &mut TcpStream) -> Vec<u8> {
    let mut all_responses = Vec::new();
    let mut buf = [0u8; 4096];
    
    // Give server time to send all frames
    thread::sleep(Duration::from_millis(50));
    
    // Set non-blocking to read all available data
    stream.set_nonblocking(true).ok();
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // EOF
            Ok(n) => all_responses.extend_from_slice(&buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read failed: {}", e),
        }
    }
    stream.set_nonblocking(false).ok();
    
    // If nothing read, fall back to single response
    if all_responses.is_empty() {
        return read_response(stream);
    }
    all_responses
}

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
    let s = std::str::from_utf8(resp).expect("invalid utf8");
    s.trim().trim_start_matches(':').parse().expect("invalid integer")
}

macro_rules! b {
    ($s:expr) => { $s.as_bytes() }
}

/// Test helper that runs a test with a fresh server on a given port
fn with_server<F>(port: u16, test_fn: F)
where
    F: FnOnce(u16),
{
    let child = start_server(port);
    // Give server time to start
    thread::sleep(Duration::from_millis(500));
    test_fn(port);
    stop_server(child);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Subscribe to one channel, publish one message, verify the push arrives.
#[test]
fn test_subscribe_receives_published_message() {
    with_server(6380, |port| {
        let channel: &[u8] = b"pubsub:test:basic";
        let payload: &[u8] = b"hello from publisher";

        let barrier = Arc::new(Barrier::new(2));
        let barrier_sub = Arc::clone(&barrier);

        let channel_sub = channel.to_vec();
        let payload_sub = payload.to_vec();

        let subscriber = thread::spawn(move || {
            let mut s = connect(port);

            send_command(&mut s, &[b!("SUBSCRIBE"), &channel_sub]);

            // Read the subscribe confirmation frame:
            // *3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:1\r\n
            let confirm = read_all_responses(&mut s);
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
            let mut s = connect(port);

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
    });
}

/// Subscribe to multiple channels, verify each gets its own confirmation frame.
#[test]
fn test_subscribe_multiple_channels() {
    with_server(6381, |port| {
        let mut s = connect(port);
        send_command(&mut s, &[
            b!("SUBSCRIBE"),
            b!("pubsub:multi:a"),
            b!("pubsub:multi:b"),
            b!("pubsub:multi:c"),
        ]);

        let resp = read_all_responses(&mut s);

        // heaven sends one *3 confirmation frame per channel
        let frame_count = count_subsequence(&resp, b"subscribe");
        assert!(
            frame_count >= 3,
            "Expected 3 subscribe confirmations, got {}: {:?}", frame_count, resp
        );
    });
}

/// After SUBSCRIBE, sending a non-pub/sub command should return an error.
#[test]
fn test_subscriber_mode_rejects_regular_commands() {
    with_server(6382, |port| {
        let mut s = connect(port);
        send_command(&mut s, &[b!("SUBSCRIBE"), b!("pubsub:mode:ch")]);
        read_all_responses(&mut s); // consume confirmation

        send_command(&mut s, &[b!("SET"), b!("pubsub:mode:k"), b!("v")]);
        let resp = read_response(&mut s);
        assert_eq!(resp[0], b'-', "Expected error in subscriber mode, got: {:?}", resp);
    });
}

/// UNSUBSCRIBE without args leaves pub/sub mode (subscriber count -> 0).
#[test]
fn test_unsubscribe_all() {
    with_server(6383, |port| {
        let mut s = connect(port);
        send_command(&mut s, &[b!("SUBSCRIBE"), b!("pubsub:unsub:a"), b!("pubsub:unsub:b")]);
        read_all_responses(&mut s); // consume confirmations

        send_command(&mut s, &[b!("UNSUBSCRIBE")]);
        let resp = read_all_responses(&mut s);

        assert!(
            contains_bytes(&resp, b"unsubscribe"),
            "Expected unsubscribe frames, got: {:?}", resp
        );
        // The final frame must carry count :0
        assert!(
            contains_bytes(&resp, b":0\r\n"),
            "Expected :0 final count, got: {:?}", resp
        );
    });
}

/// PUBLISH to a channel with no subscribers returns :0.
#[test]
fn test_publish_no_subscribers_returns_zero() {
    with_server(6384, |port| {
        let mut s = connect(port);
        send_command(&mut s, &[b!("PUBLISH"), b!("pubsub:empty:ch"), b!("nobody home")]);
        let resp = read_response(&mut s);
        assert_eq!(parse_integer(&resp), 0, "Expected :0, got: {:?}", resp);
    });
}

/// Multiple subscribers on the same channel each receive the message.
#[test]
fn test_multiple_subscribers_all_receive_message() {
    with_server(6385, |port| {
        let channel: &[u8] = b"pubsub:fan:ch";
        let n_subscribers: usize = 3;
        let barrier = Arc::new(Barrier::new(n_subscribers + 1));

        let mut handles = vec![];

        for _ in 0..n_subscribers {
            let b = Arc::clone(&barrier);
            let ch = channel.to_vec();
            handles.push(thread::spawn(move || {
                let mut s = connect(port);
                send_command(&mut s, &[b!("SUBSCRIBE"), &ch]);
                read_all_responses(&mut s); // consume confirmation

                b.wait();

                let msg = read_response(&mut s);
                assert!(
                    contains_bytes(&msg, b"message"),
                    "Subscriber missed message: {:?}", msg
                );
            }));
        }

        barrier.wait();
        thread::sleep(Duration::from_millis(50));

        let mut pub_conn = connect(port);
        send_command(&mut pub_conn, &[b!("PUBLISH"), channel, b!("broadcast")]);
        let resp = read_response(&mut pub_conn);
        assert_eq!(
            parse_integer(&resp), n_subscribers as i64,
            "Expected {} receivers, got: {:?}", n_subscribers, resp
        );

        for h in handles {
            h.join().unwrap();
        }
    });
}

/// PING is valid inside pub/sub mode.
#[test]
fn test_ping_in_subscriber_mode() {
    with_server(6386, |port| {
        let mut s = connect(port);
        send_command(&mut s, &[b!("SUBSCRIBE"), b!("pubsub:ping:ch")]);
        read_all_responses(&mut s); // consume subscription confirmation

        send_command(&mut s, &[b!("PING")]);
        let resp = read_response(&mut s);

        // In pub/sub mode Redis returns a pong response
        assert!(
            contains_bytes(&resp, b"pong") || contains_bytes(&resp, b"PONG") || contains_bytes(&resp, b"+PONG"),
            "Expected pong in subscriber mode, got: {:?}", resp
        );
    });
}