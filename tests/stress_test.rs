// Stress and concurrency tests for heaven
// These tests verify the server handles concurrent connections properly

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

/// Build and return path to the test binary
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

/// Start the heaven server
fn start_server(port: u16) -> Child {
    let binary = build_binary();
    let dbfilename = format!("stress_test_{}.rdb", port);

    Command::new(&binary)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--io-threads")
        .arg("4")
        .arg("--event-limit")
        .arg("256")
        .arg("--dbfilename")
        .arg(&dbfilename)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server")
}

/// Send a RESP command and receive response
fn send_command(host: &str, port: u16, command: &str) -> Result<String, std::io::Error> {
    let mut stream = TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(command.as_bytes())?;
    stream.flush()?;

    let mut buffer = vec![0u8; 4096];
    let n = stream.read(&mut buffer)?;
    buffer.truncate(n);

    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Concurrent SET operations from multiple threads
#[test]
fn test_concurrent_set_operations() {
    let port = 17379;
    let mut server = start_server(port);
    thread::sleep(Duration::from_millis(500));

    let num_threads = 5;
    let ops_per_thread = 20;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let b = Arc::clone(&barrier);
            let counter = Arc::clone(&success_count);
            thread::spawn(move || {
                b.wait();

                for i in 0..ops_per_thread {
                    let key = format!("thread{}_key{}", thread_id, i);
                    let value = format!("value{}_{}", thread_id, i);
                    let cmd = format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        value.len(),
                        value
                    );

                    if let Ok(resp) = send_command("127.0.0.1", port, &cmd) {
                        if resp.contains("+OK") {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_success = success_count.load(Ordering::Relaxed);
    assert_eq!(
        total_success,
        num_threads * ops_per_thread,
        "All concurrent SET operations should succeed"
    );

    // Verify all keys were written
    let verify_count = Arc::new(AtomicUsize::new(0));
    let verify_handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let counter = Arc::clone(&verify_count);
            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("thread{}_key{}", thread_id, i);
                    let expected_value = format!("value{}_{}", thread_id, i);
                    let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);

                    if let Ok(resp) = send_command("127.0.0.1", port, &cmd) {
                        if resp.contains(&expected_value) {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for h in verify_handles {
        h.join().unwrap();
    }

    let total_verified = verify_count.load(Ordering::Relaxed);
    assert_eq!(
        total_verified,
        num_threads * ops_per_thread,
        "All keys should be retrievable after concurrent writes"
    );

    let _ = server.kill();
}

/// Concurrent INCR operations on shared counter
#[test]
fn test_concurrent_incr_counter() {
    let port = 17380;
    let mut server = start_server(port);
    thread::sleep(Duration::from_millis(500));

    // Initialize counter
    let init_cmd = "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$1\r\n0\r\n";
    send_command("127.0.0.1", port, init_cmd).unwrap();

    let num_threads = 20;
    let incrs_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let b = Arc::clone(&barrier);
            thread::spawn(move || {
                b.wait();
                for _ in 0..incrs_per_thread {
                    let cmd = "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
                    let _ = send_command("127.0.0.1", port, cmd);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify final counter value
    let get_cmd = "*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n";
    let response = send_command("127.0.0.1", port, get_cmd).unwrap();

    // Parse the value from bulk string response
    let expected = num_threads * incrs_per_thread;
    assert!(
        response.contains(&expected.to_string()),
        "Counter should be {}, got: {}",
        expected,
        response
    );

    let _ = server.kill();
}

/// Mixed workload: concurrent SET, GET, DEL operations
#[test]
fn test_mixed_workload() {
    let port = 17381;
    let mut server = start_server(port);
    thread::sleep(Duration::from_millis(500));

    let num_threads = 8;
    let ops_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let b = Arc::clone(&barrier);
            let counter = Arc::clone(&success_count);
            thread::spawn(move || {
                b.wait();

                for i in 0..ops_per_thread {
                    let op = i % 3;
                    let key = format!("key{}_{}", thread_id, i);

                    let result = match op {
                        0 => {
                            // SET
                            let value = format!("value{}", i);
                            let cmd = format!(
                                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                key.len(),
                                key,
                                value.len(),
                                value
                            );
                            send_command("127.0.0.1", port, &cmd).map(|r| r.contains("+OK"))
                        }
                        1 => {
                            // GET
                            let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
                            send_command("127.0.0.1", port, &cmd).map(|_| true)
                        }
                        _ => {
                            // DEL
                            let cmd = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key);
                            send_command("127.0.0.1", port, &cmd).map(|_| true)
                        }
                    };

                    if result.unwrap_or(false) {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_success = success_count.load(Ordering::Relaxed);
    assert!(
        total_success >= num_threads * ops_per_thread * 9 / 10,
        "At least 90% of mixed workload operations should succeed, got {}/{}",
        total_success,
        num_threads * ops_per_thread
    );

    let _ = server.kill();
}

/// Stress test: rapid connection/disconnection
#[test]
fn test_rapid_connections() {
    let port = 17382;
    let mut server = start_server(port);
    thread::sleep(Duration::from_millis(500));

    let num_connections = 100;
    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_connections)
        .map(|_| {
            let counter = Arc::clone(&success_count);
            thread::spawn(move || {
                let cmd = "*1\r\n$4\r\nPING\r\n";
                if let Ok(resp) = send_command("127.0.0.1", port, cmd) {
                    if resp.contains("+PONG") {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_success = success_count.load(Ordering::Relaxed);
    assert_eq!(
        total_success, num_connections,
        "All rapid connections should succeed"
    );

    let _ = server.kill();
}

/// Pub/Sub stress test: multiple publishers and subscribers
#[test]
fn test_pubsub_stress() {
    let port = 17383;
    let mut server = start_server(port);
    thread::sleep(Duration::from_millis(500));

    let num_subscribers = 5;
    let num_publishers = 3;
    let messages_per_publisher = 10;

    let barrier = Arc::new(Barrier::new(num_subscribers + num_publishers));
    let messages_received = Arc::new(AtomicUsize::new(0));

    // Spawn subscribers
    let sub_handles: Vec<_> = (0..num_subscribers)
        .map(|sub_id| {
            let b = Arc::clone(&barrier);
            let counter = Arc::clone(&messages_received);
            thread::spawn(move || {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();

                // Subscribe
                let sub_cmd = "*2\r\n$9\r\nSUBSCRIBE\r\n$7\r\nchannel\r\n";
                stream.write_all(sub_cmd.as_bytes()).unwrap();
                stream.flush().unwrap();

                // Consume subscription confirmation
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf);

                b.wait();

                // Read messages
                let expected = num_publishers * messages_per_publisher;
                let mut received = 0;

                while received < expected {
                    buf.fill(0);
                    match stream.read(&mut buf) {
                        Ok(n) if n > 0 => {
                            let resp = String::from_utf8_lossy(&buf[..n]);
                            if resp.contains("message") {
                                received += 1;
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => break,
                    }
                }
            })
        })
        .collect();

    // Spawn publishers
    let pub_handles: Vec<_> = (0..num_publishers)
        .map(|pub_id| {
            let b = Arc::clone(&barrier);
            thread::spawn(move || {
                b.wait();

                for msg_id in 0..messages_per_publisher {
                    let msg = format!("pub{}_msg{}", pub_id, msg_id);
                    let cmd = format!(
                        "*3\r\n$7\r\nPUBLISH\r\n$7\r\nchannel\r\n${}\r\n{}\r\n",
                        msg.len(),
                        msg
                    );
                    let _ = send_command("127.0.0.1", port, &cmd);
                    thread::sleep(Duration::from_millis(10));
                }
            })
        })
        .collect();

    for h in pub_handles {
        h.join().unwrap();
    }

    // Give subscribers time to receive all messages
    thread::sleep(Duration::from_millis(500));

    for h in sub_handles {
        let _ = h.join();
    }

    let total_received = messages_received.load(Ordering::Relaxed);
    let expected_total = num_subscribers * num_publishers * messages_per_publisher;

    assert!(
        total_received >= expected_total * 9 / 10,
        "Expected at least 90% of messages to be received: {}/{}",
        total_received,
        expected_total
    );

    let _ = server.kill();
}

/// Large payload test
#[test]
fn test_large_payloads() {
    let port = 17384;
    let mut server = start_server(port);
    thread::sleep(Duration::from_millis(500));

    // Test with 1KB value
    let large_value = "x".repeat(1024);
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n$8\r\nlargekey\r\n${}\r\n{}\r\n",
        large_value.len(),
        large_value
    );

    let response = send_command("127.0.0.1", port, &set_cmd).unwrap();
    assert!(
        response.contains("+OK"),
        "SET with 1KB value should succeed"
    );

    // Retrieve and verify
    let get_cmd = "*2\r\n$3\r\nGET\r\n$8\r\nlargekey\r\n";
    let response = send_command("127.0.0.1", port, get_cmd).unwrap();
    assert!(
        response.contains(&large_value),
        "GET should return the full 1KB value"
    );

    let _ = server.kill();
}
