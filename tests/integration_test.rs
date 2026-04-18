use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

/// Helper function to build and return path to the test binary
fn build_binary() -> PathBuf {
    // Build the binary first
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

    // Return path to the built binary
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target/release/heaven");
    path
}

/// Start the heaven server as a subprocess
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

/// Helper function to send a RESP command and receive response
fn send_command(host: &str, port: u16, command: &str) -> Result<String, std::io::Error> {
    let mut stream = TcpStream::connect((host, port))?;
    stream.write_all(command.as_bytes())?;
    stream.flush()?;

    let mut buffer = vec![0u8; 4096];
    let n = stream.read(&mut buffer)?;
    buffer.truncate(n);

    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Parse a simple RESP bulk string response
fn parse_bulk_string(response: &str) -> Option<String> {
    if response.starts_with('$') {
        let lines: Vec<&str> = response.lines().collect();
        if lines.len() >= 2 {
            return Some(lines[1].to_string());
        }
    }
    None
}

/// Parse a simple RESP integer response
fn parse_integer(response: &str) -> Option<i64> {
    if response.starts_with(':') {
        return response[1..].trim().parse().ok();
    }
    None
}

/// Parse a simple RESP simple string response
fn parse_simple_string(response: &str) -> Option<String> {
    if response.starts_with('+') {
        return Some(response[1..].trim().to_string());
    }
    None
}

#[test]
fn test_ping() {
    let port = 16379;
    let mut server = start_server(port);

    // Wait for server to start
    thread::sleep(Duration::from_millis(500));

    // Test PING
    let response = send_command("127.0.0.1", port, "*1\r\n$4\r\nPING\r\n").unwrap();
    assert!(response.contains("+PONG"), "PING should return +PONG");

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_hash_operations() {
    let port = 16382;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Test HSET
    let hset_cmd = "*4\r\n$4\r\nHSET\r\n$4\r\nuser\r\n$4\r\nname\r\n$4\r\nJohn\r\n";
    let response = send_command("127.0.0.1", port, hset_cmd).unwrap();
    assert!(
        response.contains(":1") || response.contains(":0"),
        "HSET should return integer"
    );

    // Test HGET
    let hget_cmd = "*3\r\n$4\r\nHGET\r\n$4\r\nuser\r\n$4\r\nname\r\n";
    let response = send_command("127.0.0.1", port, hget_cmd).unwrap();
    assert!(
        response.contains("John"),
        "HGET should return the field value"
    );

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_list_operations() {
    let port = 16383;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Test LPUSH
    let lpush_cmd = "*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$3\r\nval\r\n";
    let response = send_command("127.0.0.1", port, lpush_cmd).unwrap();
    assert!(
        response.starts_with(':'),
        "LPUSH should return list length as integer"
    );

    // Test RPUSH
    let rpush_cmd = "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$5\r\nvalue\r\n";
    let response = send_command("127.0.0.1", port, rpush_cmd).unwrap();
    assert!(
        response.starts_with(':'),
        "RPUSH should return list length as integer"
    );

    // Test LLEN
    let llen_cmd = "*2\r\n$4\r\nLLEN\r\n$4\r\nlist\r\n";
    let response = send_command("127.0.0.1", port, llen_cmd).unwrap();
    let len = parse_integer(&response).expect("LLEN should return an integer");
    assert_eq!(len, 2, "List should have 2 elements");

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_set_operations() {
    let port = 16384;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Test SADD
    let sadd_cmd = "*3\r\n$4\r\nSADD\r\n$3\r\nset\r\n$4\r\nitem\r\n";
    let response = send_command("127.0.0.1", port, sadd_cmd).unwrap();
    assert!(response.starts_with(':'), "SADD should return integer");

    // Test SISMEMBER
    let sismember_cmd = "*3\r\n$9\r\nSISMEMBER\r\n$3\r\nset\r\n$4\r\nitem\r\n";
    let response = send_command("127.0.0.1", port, sismember_cmd).unwrap();
    assert!(
        response.contains(":1"),
        "SISMEMBER should return :1 for existing member"
    );

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_expire_ttl() {
    let port = 16385;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Set key
    let set_cmd = "*3\r\n$3\r\nSET\r\n$7\r\nexp_key\r\n$5\r\nvalue\r\n";
    send_command("127.0.0.1", port, set_cmd).unwrap();

    // Test EXPIRE
    let expire_cmd = "*3\r\n$6\r\nEXPIRE\r\n$7\r\nexp_key\r\n$3\r\n100\r\n";
    let response = send_command("127.0.0.1", port, expire_cmd).unwrap();
    assert!(response.starts_with(':'), "EXPIRE should return integer");

    // Test TTL
    let ttl_cmd = "*2\r\n$3\r\nTTL\r\n$7\r\nexp_key\r\n";
    let response = send_command("127.0.0.1", port, ttl_cmd).unwrap();
    let ttl = parse_integer(&response).expect("TTL should return an integer");
    assert!(ttl > 0 && ttl <= 100, "TTL should be positive and <= 100");

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_pubsub() {
    let port = 16386;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Test SUBSCRIBE
    let subscribe_cmd = "*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\ntest1\r\n";
    let response = send_command("127.0.0.1", port, subscribe_cmd).unwrap();
    assert!(
        response.contains("subscribe")
            || response.contains("subscribed")
            || response.starts_with('*'),
        "SUBSCRIBE should return subscription confirmation"
    );

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_set_get() {
    let port = 16380;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Test SET
    let set_cmd = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let response = send_command("127.0.0.1", port, set_cmd).unwrap();
    assert!(response.contains("+OK"), "SET should return +OK");

    // Test GET
    let get_cmd = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let response = send_command("127.0.0.1", port, get_cmd).unwrap();
    assert!(
        response.contains("$5\r\nvalue"),
        "GET should return the value"
    );

    // Cleanup
    let _ = server.kill();
}

#[test]
fn test_incr_decr() {
    let port = 16381;
    let mut server = start_server(port);

    thread::sleep(Duration::from_millis(500));

    // Set initial value
    let set_cmd = "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$1\r\n0\r\n";
    send_command("127.0.0.1", port, set_cmd).unwrap();

    // Test INCR
    let incr_cmd = "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
    let response = send_command("127.0.0.1", port, incr_cmd).unwrap();
    assert!(response.contains(":1"), "INCR should return :1");

    // Test DECR
    let decr_cmd = "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n";
    let response = send_command("127.0.0.1", port, decr_cmd).unwrap();
    assert!(response.contains(":0"), "DECR should return :0");

    // Cleanup
    let _ = server.kill();
}
