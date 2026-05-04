==> ./src/wish.rs <==
use crate::{
    temple::Temple,
    wish::{grant::Decree, util::bytes_to_usize},
};
use mio::{Token, net::TcpStream};
use std::{io::Read, sync::mpsc::Sender};

pub enum Phase {
    Idle,
    AwaitingTermCount,
    GraspingMarker,
    AwaitingBulkStringLength,
    AwaitingBulkString(usize),
}

pub struct Virtue {
    backlog: Vec<u8>,
    read_idx: usize,
    write_idx: usize,
    terms: Vec<Vec<u8>>,
    expected_terms: usize,
    phase: Phase,
}

impl Virtue {
    fn new() -> Self {
        Self {
            backlog: vec![0; 4096],
            read_idx: 0,
            write_idx: 0,
            terms: Vec::new(),
            expected_terms: 0,
            phase: Phase::Idle,
        }
    }

    fn compact(&mut self) {
        if self.read_idx > 0 {
            let len = self.write_idx - self.read_idx;
            self.backlog.copy_within(self.read_idx..self.write_idx, 0);
            self.read_idx = 0;
            self.write_idx = len;
        }
    }

    fn potentially_resize_and_read(&mut self, stream: &mut TcpStream) -> Result<bool, Sin> {
        if self.write_idx > self.backlog.len() - 1024 {
            self.compact();

            if self.write_idx > self.backlog.len() - 1024 {
                let new_size = self.backlog.len() * 2;

                if new_size > 64 * 1024 * 1024 {
                    return Err(Sin::Blasphemy);
                }

                self.backlog.resize(new_size, 0);
            }
        }

        match stream.read(&mut self.backlog[self.write_idx..]) {
            Ok(0) => return Err(Sin::Disconnected),
            Ok(n) => self.write_idx += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(true),
            Err(_) => return Err(Sin::Disconnected),
        }

        Ok(false)
    }
}

#[derive(Debug)]
pub enum Command {
    PING,
    SET,
    GET,
    EX,
    INCR,
    INCRBY,
    DECR,
    APPEND,
    STRLEN,
    EXISTS,
    DEL,
    HSET,
    HGET,
    HMGET,
    HDEL,
    HEXISTS,
    HLEN,
    LPUSH,
    LPOP,
    RPUSH,
    RPOP,
    LLEN,
    LRANGE,
    LINDEX,
    LSET,
    LREM,
    EXPIRE,
    TTL,
    SUBSCRIBE,
    PUBLISH,
    MSET,
    MGET,
    SADD,
    SREM,
    SISMEMBER,
    HGETALL,
    SMEMBERS,
    CONFIG,
    COMMAND,
}

#[derive(Debug)]
pub enum Sacrilege {
    IncorrectNumberOfArguments(Command),
    IncorrectUsage(Command),
    UnknownCommand,
    SubscriberOnlyMode,
}

pub enum InfoType {
    Ok,
    Pong,
    Command,
}

pub enum Response {
    Error(Sacrilege),
    Info(InfoType),
    BulkString(Option<Vec<u8>>),
    BulkStringArray(Option<Vec<Option<Vec<u8>>>>),
    Amount(u32),
    Number(i64),
    Length(usize),
    SubscribedChannels(Vec<(Vec<u8>, usize)>),
    UnsubscribedChannels(Option<Vec<(Vec<u8>, usize)>>),
}

pub struct Pilgrim {
    pub stream: TcpStream,
    pub virtue: Option<Virtue>,
    pub tx: Sender<Decree>,
}

#[derive(Debug)]
pub enum Sin {
    Utf8Error,
    ParseError,
    Disconnected,
    Blasphemy,
}

pub mod grant;
pub mod util;

pub fn wish(pilgrim: &mut Pilgrim, mut temple: Temple, token: Token) -> Result<(), Sin> {
    let virtue = pilgrim.virtue.get_or_insert_with(Virtue::new);

    if virtue.potentially_resize_and_read(&mut pilgrim.stream)? {
        return Ok(());
    }

    loop {
        let active_window = &virtue.backlog[virtue.read_idx..virtue.write_idx];

        if active_window.is_empty() {
            break;
        }

        match virtue.phase {
            Phase::Idle => {
                if active_window[0] == b'*' {
                    virtue.phase = Phase::AwaitingTermCount;
                    virtue.read_idx += 1;
                } else {
                    return Err(Sin::Blasphemy);
                }
            }
            Phase::AwaitingTermCount => {
                if let Some(index) = util::find_crlf(active_window) {
                    virtue.expected_terms = bytes_to_usize(&active_window[..index])?;
                    virtue.phase = Phase::GraspingMarker;
                    virtue.read_idx += index + 2;
                } else {
                    if virtue.potentially_resize_and_read(&mut pilgrim.stream)? {
                        return Ok(());
                    }

                    continue;
                }
            }
            Phase::GraspingMarker => {
                if active_window[0] == b'$' {
                    virtue.phase = Phase::AwaitingBulkStringLength;
                    virtue.read_idx += 1;
                } else {
                    if virtue.potentially_resize_and_read(&mut pilgrim.stream)? {
                        return Ok(());
                    }

                    continue;
                }
            }
            Phase::AwaitingBulkStringLength => {
                if let Some(index) = util::find_crlf(active_window) {
                    let len = bytes_to_usize(&active_window[..index])?;
                    virtue.phase = Phase::AwaitingBulkString(len);
                    virtue.read_idx += index + 2;
                } else {
                    if virtue.potentially_resize_and_read(&mut pilgrim.stream)? {
                        return Ok(());
                    }

                    continue;
                }
            }
            Phase::AwaitingBulkString(len) => {
                if active_window.len() >= len + 2 {
                    if active_window[len] != b'\r' || active_window[len + 1] != b'\n' {
                        return Err(Sin::Blasphemy);
                    }

                    virtue.terms.push(active_window[..len].to_vec());
                    virtue.read_idx += len + 2;
                    virtue.phase = Phase::GraspingMarker;

                    if virtue.terms.len() == virtue.expected_terms {
                        let terms = std::mem::take(&mut virtue.terms);

                        grant::grant(terms, &mut temple, pilgrim.tx.clone(), token);

                        virtue.phase = Phase::Idle;
                    }
                } else {
                    if virtue.potentially_resize_and_read(&mut pilgrim.stream)? {
                        return Ok(());
                    }

                    continue;
                }
            }
        }
    }

    Ok(())
}

==> ./src/egress/send.rs <==
use mio::net::TcpStream;
use std::io::Write;

use crate::wish::{Command, InfoType, Response, Sacrilege, Sin, grant::Gift};

pub fn send(stream: &mut TcpStream, gift: Gift, response: &mut Vec<u8>) -> Result<(), Sin> {
    response.clear();
    let mut itoa_buf = itoa::Buffer::new();

    match gift.response {
        Response::Info(InfoType::Ok) => {
            response.extend_from_slice(b"+OK\r\n");
        }
        Response::Info(InfoType::Pong) => {
            response.extend_from_slice(b"+PONG\r\n");
        }
        Response::Info(InfoType::Command) => {
            response.extend_from_slice(
                b"*32\r\n\
                *6\r\n$3\r\nset\r\n:-3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$6\r\nappend\r\n:3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nincr\r\n:2\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\ndecr\r\n:2\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$6\r\nstrlen\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$5\r\nlpush\r\n:-3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$5\r\nrpush\r\n:-3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nlpop\r\n:-2\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nrpop\r\n:-2\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$6\r\nlrange\r\n:4\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nlrem\r\n:4\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$6\r\nlindex\r\n:3\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nllen\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nlset\r\n:4\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nhset\r\n:-4\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$5\r\nhmget\r\n:-3\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nhget\r\n:3\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nhdel\r\n:-3\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$7\r\nhexists\r\n:3\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nhlen\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$7\r\nhgetall\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nsadd\r\n:-3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$4\r\nsrem\r\n:-3\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$9\r\nsismember\r\n:3\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$8\r\nsmembers\r\n:2\r\n*2\r\n+readonly\r\n+sort_for_script\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$6\r\nexists\r\n:-2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n-1\r\n:1\r\n\
                *6\r\n$3\r\ndel\r\n:-2\r\n*2\r\n+write\r\n+fast\r\n:1\r\n-1\r\n:1\r\n\
                *6\r\n$3\r\nttl\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$6\r\nexpire\r\n:3\r\n*2\r\n+write\r\n+fast\r\n:1\r\n:1\r\n:1\r\n\
                *6\r\n$7\r\npublish\r\n:3\r\n*3\r\n+pubsub\r\n+loading\r\n+stale\r\n:0\r\n:0\r\n:0\r\n\
                *6\r\n$4\r\nping\r\n:-1\r\n*2\r\n+stale\r\n+fast\r\n:0\r\n:0\r\n:0\r\n");
        }
        Response::BulkString(bulk_string) => match bulk_string {
            Some(value) => {
                response.push(b'$');
                response.extend_from_slice(itoa_buf.format(value.len()).as_bytes());
                response.extend_from_slice(b"\r\n");
                response.extend_from_slice(&value);
                response.extend_from_slice(b"\r\n");
            }
            None => {
                response.extend_from_slice(b"$-1\r\n");
            }
        },
        Response::BulkStringArray(bulk_string_array) => match bulk_string_array {
            Some(bulk_string_array) => {
                response.push(b'*');
                response.extend_from_slice(itoa_buf.format(bulk_string_array.len()).as_bytes());
                response.extend_from_slice(b"\r\n");

                for bulk_string in bulk_string_array {
                    match bulk_string {
                        Some(value) => {
                            response.push(b'$');
                            response.extend_from_slice(itoa_buf.format(value.len()).as_bytes());
                            response.extend_from_slice(b"\r\n");
                            response.extend_from_slice(&value);
                            response.extend_from_slice(b"\r\n");
                        }
                        None => {
                            response.extend_from_slice(b"$-1\r\n");
                        }
                    }
                }
            }
            None => {
                response.extend_from_slice(b"$-1\r\n");
            }
        },
        Response::Amount(amount) => {
            response.push(b':');
            response.extend_from_slice(itoa_buf.format(amount).as_bytes());
            response.extend_from_slice(b"\r\n");
        }
        Response::Number(number) => {
            response.push(b':');
            response.extend_from_slice(itoa_buf.format(number).as_bytes());
            response.extend_from_slice(b"\r\n");
        }
        Response::Length(length) => {
            response.push(b':');
            response.extend_from_slice(itoa_buf.format(length).as_bytes());
            response.extend_from_slice(b"\r\n");
        }
        Response::SubscribedChannels(subscribed_channels) => {
            for (subscribed_channel, count) in subscribed_channels {
                response.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n$");
                response.extend_from_slice(itoa_buf.format(subscribed_channel.len()).as_bytes());
                response.extend_from_slice(b"\r\n");
                response.extend_from_slice(&subscribed_channel);
                response.extend_from_slice(b"\r\n:");

                response.extend_from_slice(itoa_buf.format(count).as_bytes());
                response.extend_from_slice(b"\r\n");
            }
        }
        Response::UnsubscribedChannels(unsubscribed_channels) => match unsubscribed_channels {
            Some(unsubscribed_channels) => {
                for (unsubscribed_channel, count) in unsubscribed_channels {
                    response.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                    response
                        .extend_from_slice(itoa_buf.format(unsubscribed_channel.len()).as_bytes());
                    response.extend_from_slice(b"\r\n");
                    response.extend_from_slice(&unsubscribed_channel);
                    response.extend_from_slice(b"\r\n:");

                    response.extend_from_slice(itoa_buf.format(count).as_bytes());
                    response.extend_from_slice(b"\r\n");
                }
            }
            None => {
                response.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
            }
        },
        Response::Error(sacrilege) => match sacrilege {
            Sacrilege::UnknownCommand => {
                response.extend_from_slice(b"-ERR unknown command\r\n");
            }
            Sacrilege::IncorrectUsage(command) => match command {
                Command::INCR | Command::DECR | Command::INCRBY => {
                    response.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                }
                Command::LSET | Command::LINDEX => {
                    response.extend_from_slice(b"-ERR index out of range\r\n");
                }
                Command::CONFIG => {
                    response.extend_from_slice(b"-ERR Unknown Command after 'config'\r\n");
                }
                _ => {
                    response.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            },
            Sacrilege::IncorrectNumberOfArguments(command) => match command {
                Command::PING => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'ping' command\r\n"),
                Command::SET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'set' command\r\n"),
                Command::GET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'get' command\r\n"),
                Command::EX => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'ex' command\r\n"),
                Command::INCR => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'incr' command\r\n"),
                Command::INCRBY => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'incrby' command\r\n"),
                Command::DECR => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'decr' command\r\n"),
                Command::APPEND => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'append' command\r\n"),
                Command::STRLEN => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'strlen' command\r\n"),
                Command::EXISTS => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'exists' command\r\n"),
                Command::DEL => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'del' command\r\n"),
                Command::HSET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hset' command\r\n"),
                Command::HGET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hget' command\r\n"),
                Command::HMGET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hmget' command\r\n"),
                Command::HDEL => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hdel' command\r\n"),
                Command::HEXISTS => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hexists' command\r\n"),
                Command::HLEN => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hlen' command\r\n"),
                Command::LPUSH => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'lpush' command\r\n"),
                Command::LPOP => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'lpop' command\r\n"),
                Command::RPUSH => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'rpush' command\r\n"),
                Command::RPOP => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'rpop' command\r\n"),
                Command::LLEN => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'llen' command\r\n"),
                Command::LRANGE => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'lrange' command\r\n"),
                Command::LINDEX => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'lindex' command\r\n"),
                Command::LSET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'lset' command\r\n"),
                Command::LREM => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'lrem' command\r\n"),
                Command::EXPIRE => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'expire' command\r\n"),
                Command::TTL => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'ttl' command\r\n"),
                Command::SUBSCRIBE => response.extend_from_slice(
                    b"-ERR wrong number of arguments for 'subscribe' command\r\n",
                ),
                Command::PUBLISH => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'publish' command\r\n"),
                Command::MSET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'mset' command\r\n"),
                Command::MGET => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'mget' command\r\n"),
                Command::SADD => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'sadd' command\r\n"),
                Command::SREM => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'srem' command\r\n"),
                Command::SISMEMBER => response.extend_from_slice(
                    b"-ERR wrong number of arguments for 'sismember' command\r\n",
                ),
                Command::HGETALL => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'hgetall' command\r\n"),
                Command::SMEMBERS => response.extend_from_slice(
                    b"-ERR wrong number of arguments for 'smembers' command\r\n",
                ),
                Command::CONFIG => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'config' command\r\n"),
                Command::COMMAND => response
                    .extend_from_slice(b"-ERR wrong number of arguments for 'command' command\r\n"),
            },
            Sacrilege::SubscriberOnlyMode => response.extend_from_slice(
                b"-ERR only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT allowed in this context\r\n",
            ),
        },
    }

    stream.write_all(response).map_err(|_| Sin::Disconnected)?;

    Ok(())
}

==> ./src/server.rs <==
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;

use heaven::choir::Choir;
use heaven::egress;
use heaven::temple::Temple;
use heaven::temple::soul::ServerError;
use heaven::wish::grant::Decree;
use heaven::wish::{self, Pilgrim};

use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};

/// Errors that can occur during server startup
#[derive(Debug)]
pub enum ServerStartupError {
    InvalidIpAddress(String),
    PollCreationFailed(std::io::Error),
    BindFailed(std::io::Error),
    InvalidPath(PathBuf),
}

impl std::fmt::Display for ServerStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIpAddress(ip) => write!(f, "Invalid IPv4 address: {}", ip),
            Self::PollCreationFailed(e) => write!(f, "Failed to create poll instance: {}", e),
            Self::BindFailed(e) => write!(f, "Failed to bind to address: {}", e),
            Self::InvalidPath(path) => write!(f, "Path contains invalid UTF-8: {:?}", path),
        }
    }
}

impl std::error::Error for ServerStartupError {}

pub fn run(
    ipv4_address: &str,
    port: u16,
    io_threads: usize,
    event_capacity: usize,
    dir: PathBuf,
    dbfilename: &str,
    max_memory: u64,
    append_only: &str,
) -> Result<(), ServerStartupError> {
    let ipv4_addr = Ipv4Addr::from_str(ipv4_address)
        .map_err(|_| ServerStartupError::InvalidIpAddress(ipv4_address.to_string()))?;
    let socket_addr_v4 = SocketAddrV4::new(ipv4_addr, port);
    let socket_addr = SocketAddr::V4(socket_addr_v4);

    let mut poll = Poll::new().map_err(ServerStartupError::PollCreationFailed)?;

    let mut listener = TcpListener::bind(socket_addr).map_err(ServerStartupError::BindFailed)?;

    const SERVER: Token = Token(0);

    let mut events = Events::with_capacity(event_capacity);

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)
        .map_err(|e| {
            eprintln!("Failed to register listener with poll: {}", e);
            ServerStartupError::PollCreationFailed(e)
        })?;

    let mut ingress_map: HashMap<Token, Pilgrim> = HashMap::new();

    let mut pilgrim_counter = 1;

    let ingress_choir = Choir::new(io_threads);

    let mut itoa_buf = itoa::Buffer::new();

    let dir_str = dir
        .to_str()
        .ok_or_else(|| ServerStartupError::InvalidPath(dir.clone()))?;

    let temple = Temple::worship(
        dir_str.into(),
        dbfilename.into(),
        ipv4_address.into(),
        itoa_buf.format(port).into(),
        itoa_buf.format(io_threads).into(),
        itoa_buf.format(event_capacity).into(),
        itoa_buf.format(max_memory).into(),
        append_only.into(),
    );

    let mut server_temple = temple.sanctify();

    let (ingress_tx, ingress_rx) = std::sync::mpsc::channel::<(Token, Pilgrim)>();
    let (egress_tx, egress_rx) = std::sync::mpsc::channel();
    let (pilgrim_tx, pilgrim_rx) = std::sync::mpsc::channel::<Decree>();

    let shutdown_tx = egress_tx.clone();

    std::thread::spawn(move || {
        egress::egress(pilgrim_rx, egress_tx);
    });

    if ctrlc::set_handler(move || {
        let (server_shutdown_tx, server_shutdown_rx) =
            std::sync::mpsc::channel::<Result<(), ServerError>>();

        server_temple.save(server_shutdown_tx, SERVER);

        if let Ok(Ok(())) = server_shutdown_rx.recv() {
            println!("Database snapshot saved successfuly");
        } else {
            println!("Couldn't save database snapshot");
        }

        if shutdown_tx.send(SERVER).is_err() {
            eprintln!("Ctrlc handler failed");
        }
    })
    .is_err()
    {
        eprintln!("Failed to set ctrlc handler");
    };

    loop {
        while let Ok(token) = egress_rx.try_recv() {
            if token == SERVER {
                std::process::exit(0);
            }

            if let Some(mut pilgrim) = ingress_map.remove(&token)
                && poll.registry().deregister(&mut pilgrim.stream).is_err()
            {
                eprintln!("deregister() failed")
            }
        }

        if poll
            .poll(&mut events, Some(std::time::Duration::from_millis(100)))
            .is_err()
        {
            eprintln!("poll() failed");
        }

        while let Ok((token, mut pilgrim)) = ingress_rx.try_recv() {
            if poll
                .registry()
                .reregister(
                    &mut pilgrim.stream,
                    token,
                    Interest::READABLE | Interest::WRITABLE,
                )
                .is_err()
            {
                // eprintln!("reregister() failed");
            }

            ingress_map.insert(token, pilgrim);
        }

        for event in &events {
            let token = event.token();
            match token {
                SERVER => loop {
                    match listener.accept() {
                        Ok((mut stream, _address)) => {
                            let pilgrim_token = Token(pilgrim_counter);

                            if poll
                                .registry()
                                .register(
                                    &mut stream,
                                    pilgrim_token,
                                    Interest::READABLE | Interest::WRITABLE,
                                )
                                .is_err()
                            {
                                eprintln!("register() failed");
                            }

                            let std_stream: TcpStream = stream.into();
                            let std_stream_clone = match std_stream.try_clone() {
                                Ok(clone) => clone,
                                Err(e) => {
                                    eprintln!(
                                        "Failed to clone socket for client {:?}: {}",
                                        pilgrim_token, e
                                    );
                                    continue;
                                }
                            };

                            let ingress_mio = mio::net::TcpStream::from_std(std_stream);
                            let egress_mio = mio::net::TcpStream::from_std(std_stream_clone);

                            pilgrim_counter += 1;

                            ingress_map.insert(
                                pilgrim_token,
                                Pilgrim {
                                    stream: ingress_mio,
                                    virtue: None,
                                    tx: pilgrim_tx.clone(),
                                },
                            );

                            if pilgrim_tx
                                .send(Decree::Welcome(pilgrim_token, egress_mio))
                                .is_err()
                            {
                                eprintln!(
                                    "Failed to send welcome to egress for client {:?}: channel closed",
                                    pilgrim_token
                                );
                                ingress_map.remove(&pilgrim_token);
                            }
                        }
                        Err(err) => {
                            if err.kind() == ErrorKind::WouldBlock {
                                break;
                            }
                        }
                    }
                },

                Token(token_number) => {
                    if let Some(mut pilgrim) = ingress_map.remove(&Token(token_number)) {
                        let sanctum = temple.sanctify();
                        let tx = ingress_tx.clone();

                        ingress_choir.sing(move || {
                            match wish::wish(&mut pilgrim, sanctum, Token(token_number)) {
                                Ok(_) => {
                                    if tx.send((mio::Token(token_number), pilgrim)).is_err() {
                                 eprintln!("Failed to return client {:?} to event loop: channel closed", Token(token_number));
                                    }
                                }
                                Err(_e) => {
                                    // eprintln!("{:?}", e);
                                }
                            }
                        });
                    }
                }
            }
        }
    }
}

==> ./src/tests.rs <==
//unit tests
mod soul_test;

==> ./src/lib.rs <==
pub mod choir;
pub mod egress;
pub mod temple;
pub mod wish;

#[cfg(test)]
mod tests;

==> ./src/wish/util.rs <==
use crate::wish::Sin;

pub fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|w| w == b"\r\n")
}

pub fn bytes_to_i32(bytes: &[u8]) -> Result<i32, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let (is_neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };

    let mut result = 0i32;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as i32))
            .ok_or(Sin::ParseError)?;
    }

    if is_neg {
        result.checked_neg().ok_or(Sin::ParseError)
    } else {
        Ok(result)
    }
}

pub fn bytes_to_u64(bytes: &[u8]) -> Result<u64, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let mut result = 0u64;

    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as u64))
            .ok_or(Sin::ParseError)?;
    }

    Ok(result)
}

pub fn bytes_to_i64(bytes: &[u8]) -> Result<i64, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let (is_neg, start) = if bytes[0] == b'-' {
        (true, 1)
    } else {
        (false, 0)
    };

    let mut result = 0i64;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as i64))
            .ok_or(Sin::ParseError)?;
    }

    if is_neg {
        result.checked_neg().ok_or(Sin::ParseError)
    } else {
        Ok(result)
    }
}

pub fn bytes_to_usize(bytes: &[u8]) -> Result<usize, Sin> {
    if bytes.is_empty() {
        return Err(Sin::ParseError);
    }

    let mut result = 0usize;

    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(Sin::ParseError);
        }

        result = result
            .checked_mul(10)
            .and_then(|r| r.checked_add((b - b'0') as usize))
            .ok_or(Sin::ParseError)?;
    }

    Ok(result)
}

==> ./src/wish/grant/del.rs <==
use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn del(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::DEL)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.del(
        terms_iter.collect(),
        tx,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}

==> ./src/wish/grant/subscribe.rs <==
use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::sync::mpsc::Sender;

pub fn subscribe(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(
                    Command::SUBSCRIBE,
                )),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.subscribe(tx, terms_iter.collect(), token);
}

==> ./src/wish/grant/decr.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn decr(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::DECR)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.decr(
            key,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::DECR)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/hexists.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn hexists(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HEXISTS)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(field)) = (terms_iter.next(), terms_iter.next()) {
        temple.hexists(
            tx,
            key,
            field,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HEXISTS)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/config.rs <==
use std::sync::mpsc::Sender;

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn config(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::CONFIG)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let Some(command) = terms_iter.next() else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::CONFIG)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    if !command.eq_ignore_ascii_case(b"GET") {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::CONFIG)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    temple.config_get(tx, token, terms_iter.collect());
}

==> ./src/wish/grant/publish.rs <==
use std::sync::mpsc::Sender;

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn publish(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::PUBLISH)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(event), Some(message)) = (terms_iter.next(), terms_iter.next()) {
        temple.publish(tx, event, message, token);
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::PUBLISH)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/rpop.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_usize,
    },
};

pub fn rpop(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() > 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::RPOP)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        if let Some(count) = terms_iter.next() {
            if let Ok(count) = bytes_to_usize(&count) {
                temple.rpop_m(
                    tx,
                    key,
                    count,
                    token,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                );

                return;
            }

            if tx
                .send(Decree::Deliver(Gift {
                    token,
                    response: Response::Error(Sacrilege::IncorrectUsage(Command::RPOP)),
                }))
                .is_err()
            {
                eprintln!("Failed to send command response: channel closed");
            };

            return;
        }

        temple.rpop(
            tx,
            key,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::RPOP)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/smembers.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn smembers(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SMEMBERS)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.smembers(
            key,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SMEMBERS)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/hset.rs <==
use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn hset(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    let terms_len = terms.len();

    if terms_len < 4 || !terms_len.is_multiple_of(2) {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HSET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        let mut field_value_pairs = Vec::new();

        while let (Some(field), Some(value)) = (terms_iter.next(), terms_iter.next()) {
            field_value_pairs.push((field, value));
        }

        temple.hset(
            key,
            field_value_pairs,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/hdel.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn hdel(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HDEL)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.hdel(
            tx,
            key,
            terms_iter.collect(),
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/strlen.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn strlen(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::STRLEN)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.strlen(
            key,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::STRLEN)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/hget.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn hget(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HGET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(field)) = (terms_iter.next(), terms_iter.next()) {
        temple.hget(
            tx,
            key,
            field,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HGET)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/incrby.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_i64,
    },
};

pub fn incrby(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::INCRBY)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let Some(key) = terms_iter.next() else {
        return;
    };

    let Some(number) = terms_iter.next() else {
        return;
    };

    let Ok(number) = bytes_to_i64(&number) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::INCRBY)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
        return;
    };

    temple.incrby(
        key,
        number,
        tx,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );

    // if let Some(key) = terms_iter.next() {
    //     temple.incr(
    //         key,
    //         tx,
    //         token,
    //         SystemTime::now()
    //             .duration_since(UNIX_EPOCH)
    //             .map(|d| d.as_secs())
    //             .unwrap_or(0),
    //     );
    // } else if tx
    //     .send(Decree::Deliver(Gift {
    //         token,
    //         response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::INCR)),
    //     }))
    //     .is_err()
    // {
    //     eprintln!("Failed to send command response: channel closed");
    // }
}

==> ./src/wish/grant/set.rs <==
use mio::Token;

use crate::{
    temple::{Temple, soul::Value},
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_u64,
    },
};

use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn set(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() > 5 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed")
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(value)) = (terms_iter.next(), terms_iter.next()) {
        match terms_iter.next() {
            Some(command) => {
                if command.eq_ignore_ascii_case(b"EX") {
                    let Some(expiry) = terms_iter.next() else {
                        if tx
                            .send(Decree::Deliver(Gift {
                                token,
                                response: Response::Error(Sacrilege::IncorrectUsage(Command::SET)),
                            }))
                            .is_err()
                        {
                            eprintln!("Failed to send command response: channel closed")
                        };

                        return;
                    };

                    let Ok(expiry) = bytes_to_u64(&expiry) else {
                        if tx
                            .send(Decree::Deliver(Gift {
                                token,
                                response: Response::Error(Sacrilege::IncorrectUsage(Command::SET)),
                            }))
                            .is_err()
                        {
                            eprintln!("Failed to send command response: channel closed")
                        };

                        return;
                    };

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);

                    temple.set(key, (Value::String(value), Some(now + expiry)), tx, token);
                } else if tx
                    .send(Decree::Deliver(Gift {
                        token,
                        response: Response::Error(Sacrilege::IncorrectNumberOfArguments(
                            Command::SET,
                        )),
                    }))
                    .is_err()
                {
                    eprintln!("Failed to send command response: channel closed")
                }
            }
            None => {
                temple.set(key, (Value::String(value), None), tx, token);
            }
        }
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SET)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed")
    }
}

==> ./src/wish/grant/lindex.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_i32,
    },
};

pub fn lindex(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3
        && tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LINDEX)),
            }))
            .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(index)) = (terms_iter.next(), terms_iter.next()) {
        if let Ok(index) = bytes_to_i32(&index) {
            temple.lindex(
                tx,
                key,
                index,
                token,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            );
        } else if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::LINDEX)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LINDEX)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/hlen.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn hlen(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HLEN)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.hlen(
            tx,
            key,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HLEN)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/get.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn get(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::GET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.get(
            key,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::GET)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/incr.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn incr(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::INCR)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.incr(
            key,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::INCR)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/srem.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn srem(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.srem(
            tx,
            key,
            terms_iter.collect(),
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/ping.rs <==
use crate::wish::{Command, Sacrilege};
use std::sync::mpsc::Sender;

use mio::Token;

use crate::wish::{
    InfoType, Response,
    grant::{Decree, Gift},
};

pub fn ping(terms: Vec<Vec<u8>>, tx: Sender<Decree>, token: Token) {
    if terms.len() != 1
        && tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::PING)),
            }))
            .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
        return;
    }

    if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Info(InfoType::Pong),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    };
}

==> ./src/wish/grant/llen.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn llen(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2
        && tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LLEN)),
            }))
            .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.llen(
            tx,
            key,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LLEN)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/exists.rs <==
use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn exists(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::EXISTS)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.exists(
        terms_iter.collect(),
        tx,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}

==> ./src/wish/grant/expire.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_u64,
    },
};

pub fn expire(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::EXPIRE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let (Some(key), Some(expiry)) = (terms_iter.next(), terms_iter.next()) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::EXPIRE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    let Ok(expiry) = bytes_to_u64(&expiry) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::EXPIRE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    temple.expire(tx, key, now + expiry, token, now);
}

==> ./src/wish/grant/unsubscribe.rs <==
use std::sync::mpsc::Sender;

use mio::Token;

use crate::{temple::Temple, wish::grant::Decree};

pub fn unsubscribe(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.unsubscribe(tx, token, terms_iter.collect());
}

==> ./src/wish/grant/lpop.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_usize,
    },
};

pub fn lpop(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() > 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LPOP)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        if let Some(count) = terms_iter.next() {
            if let Ok(count) = bytes_to_usize(&count) {
                temple.lpop_m(
                    tx,
                    key,
                    count,
                    token,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                );

                return;
            }

            if tx
                .send(Decree::Deliver(Gift {
                    token,
                    response: Response::Error(Sacrilege::IncorrectUsage(Command::LPOP)),
                }))
                .is_err()
            {
                eprintln!("Failed to send command response: channel closed");
            };

            return;
        }

        temple.lpop(
            tx,
            key,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LPOP)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/lrem.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_i32,
    },
};

pub fn lrem(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 4 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let (Some(key), Some(count), Some(element)) =
        (terms_iter.next(), terms_iter.next(), terms_iter.next())
    else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    };

    let Ok(index) = bytes_to_i32(&count) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::LREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    };

    temple.lrem(
        tx,
        key,
        index,
        element,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}

==> ./src/wish/grant/lpush.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn lpush(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LPUSH)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.lpush(
            tx,
            key,
            terms_iter.collect(),
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/ttl.rs <==
use std::{sync::mpsc::Sender, time::SystemTime};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn ttl(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::TTL)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let Some(key) = terms_iter.next() else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::TTL)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    temple.ttl(tx, key, token, SystemTime::now());
}

==> ./src/wish/grant/sadd.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn sadd(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SADD)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.sadd(
            tx,
            key,
            terms_iter.collect(),
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/mset.rs <==
use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::sync::mpsc::Sender;

pub fn mset(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    let terms_len = terms.len();

    if terms_len < 3 || terms_len.is_multiple_of(2) {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::MSET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.mset(terms_iter, tx, token);
}

==> ./src/wish/grant/sismember.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn sismember(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(
                    Command::SISMEMBER,
                )),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(value)) = (terms_iter.next(), terms_iter.next()) {
        temple.sismember(
            tx,
            key,
            value,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SISMEMBER)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/mget.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn mget(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::MGET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.mget(
        terms_iter,
        tx,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}

==> ./src/wish/grant/lset.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_i32,
    },
};

pub fn lset(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 4 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LSET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let (Some(key), Some(index), Some(element)) =
        (terms_iter.next(), terms_iter.next(), terms_iter.next())
    else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LSET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    };

    let Ok(index) = bytes_to_i32(&index) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::LSET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    };

    temple.lset(
        tx,
        key,
        index,
        element,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}

==> ./src/wish/grant/command.rs <==
use crate::wish::{Command, Sacrilege};
use std::sync::mpsc::Sender;

use mio::Token;

use crate::wish::{
    InfoType, Response,
    grant::{Decree, Gift},
};

pub fn command(terms: Vec<Vec<u8>>, tx: Sender<Decree>, token: Token) {
    if terms.len() != 1 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::COMMAND)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Info(InfoType::Command),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    };
}

==> ./src/wish/grant/rpush.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn rpush(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::RPUSH)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.rpush(
            tx,
            key,
            terms_iter.collect(),
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/hmget.rs <==
use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use mio::Token;
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn hmget(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() < 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HMGET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.hmget(
            tx,
            key,
            terms_iter.collect(),
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
}

==> ./src/wish/grant/hgetall.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn hgetall(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HGETALL)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let Some(key) = terms_iter.next() {
        temple.hgetall(
            key,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::HGETALL)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/append.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn append(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::APPEND)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(value)) = (terms_iter.next(), terms_iter.next()) {
        temple.append(
            key,
            value,
            tx,
            token,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::APPEND)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/wish/grant/lrange.rs <==
use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_i32,
    },
};

pub fn lrange(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 4 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LRANGE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let (Some(key), Some(starting_index), Some(ending_index)) =
        (terms_iter.next(), terms_iter.next(), terms_iter.next())
    else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LRANGE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    let (Ok(starting_index), Ok(ending_index)) =
        (bytes_to_i32(&starting_index), bytes_to_i32(&ending_index))
    else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::LRANGE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    temple.lrange(
        tx,
        key,
        starting_index,
        ending_index,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}

==> ./src/wish/grant.rs <==
use mio::Token;

use crate::{
    temple::Temple,
    wish::{InfoType, Response, Sacrilege},
};

use std::sync::mpsc::Sender;

mod append;
mod command;
mod config;
mod decr;
mod del;
mod exists;
mod expire;
mod get;
mod hdel;
mod hexists;
mod hget;
mod hgetall;
mod hlen;
mod hmget;
mod hset;
mod incr;
mod incrby;
mod lindex;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod lrem;
mod lset;
mod mget;
mod mset;
mod ping;
mod publish;
mod rpop;
mod rpush;
mod sadd;
mod set;
mod sismember;
mod smembers;
mod srem;
mod strlen;
mod subscribe;
mod ttl;
mod unsubscribe;

pub struct Gift {
    pub token: mio::Token,
    pub response: Response,
}

pub enum Decree {
    Welcome(Token, mio::net::TcpStream),
    Deliver(Gift),
    Broadcast(Token, Vec<u8>, Vec<u8>, Vec<Token>),
}

pub fn grant(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    let cmd = &terms[0];

    if cmd.eq_ignore_ascii_case(b"SET") {
        set::set(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"GET") {
        get::get(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"PING") {
        ping::ping(terms, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"DEL") {
        del::del(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"EXISTS") {
        exists::exists(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"INCRBY") {
        incrby::incrby(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"INCR") {
        incr::incr(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"DECR") {
        decr::decr(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"APPEND") {
        append::append(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HSET") {
        hset::hset(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HGET") {
        hget::hget(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HMGET") {
        hmget::hmget(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"STRLEN") {
        strlen::strlen(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HDEL") {
        hdel::hdel(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HEXISTS") {
        hexists::hexists(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HLEN") {
        hlen::hlen(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LPUSH") {
        lpush::lpush(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LPOP") {
        lpop::lpop(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"RPUSH") {
        rpush::rpush(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"RPOP") {
        rpop::rpop(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LLEN") {
        llen::llen(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LRANGE") {
        lrange::lrange(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LINDEX") {
        lindex::lindex(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LSET") {
        lset::lset(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"LREM") {
        lrem::lrem(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"EXPIRE") {
        expire::expire(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"TTL") {
        ttl::ttl(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") {
        subscribe::subscribe(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"PUBLISH") {
        publish::publish(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"MSET") {
        mset::mset(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"MGET") {
        mget::mget(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"SADD") {
        sadd::sadd(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"SREM") {
        srem::srem(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"SISMEMBER") {
        sismember::sismember(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"HGETALL") {
        hgetall::hgetall(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"SMEMBERS") {
        smembers::smembers(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
        unsubscribe::unsubscribe(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"CONFIG") {
        config::config(terms, temple, tx, token);
    } else if cmd.eq_ignore_ascii_case(b"QUIT") {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Info(InfoType::Ok),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };
    } else if cmd.eq_ignore_ascii_case(b"COMMAND") {
        command::command(terms, tx, token);

        // if tx
        //     .send(Decree::Deliver(Gift {
        //         token,
        //         response: Response::Info(InfoType::Ok),
        //     }))
        //     .is_err()
        // {
        //     eprintln!("Failed to send command response: channel closed");
        // };
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::UnknownCommand),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}

==> ./src/temple.rs <==
use crate::temple::{
    BroadcastCommand::{Publish, Subscribe, Unsubscribe},
    ClientCommandType::{Broadcast, Database},
    ServerCommand::{GetFilePath, Save},
};
use crate::temple::{
    CommandType::{Client, Server},
    DatabaseCommand::{
        Append, ConfigGet, Decr, Del, Exists, Expire, Get, Hdel, Hexists, Hget, Hgetall, Hlen,
        Hmget, Hset, Incr, Lindex, Llen, Lpop, LpopM, Lpush, Lrange, Lrem, Lset, Mget, Mset, Rpop,
        RpopM, Rpush, Sadd, Set, Sismember, Smembers, Srem, Strlen, Ttl, Incrby
    },
};

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::vec::IntoIter;
use std::{collections::HashMap, time::SystemTime};

use mio::Token;
use rkyv::api::low::deserialize;
use rkyv::rancor::Error;

use crate::temple::soul::ServerError;
use crate::wish::grant::{Decree, Gift};
use crate::wish::{InfoType, Response, Sacrilege};

pub struct EventMap(HashMap<Token, HashSet<Vec<u8>>>);
pub struct ClientMap(HashMap<Vec<u8>, HashSet<Token>>);

pub mod soul;

use soul::{ArchivedSoul, Soul, Value};

impl Default for ClientMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientMap {
    pub fn new() -> Self {
        ClientMap(HashMap::new())
    }

    pub fn subscribe(&mut self, token: Token, events: Vec<Vec<u8>>) {
        for event in events {
            match self.0.get_mut(&event) {
                Some(set) => {
                    set.insert(token);
                }
                None => {
                    let mut set = HashSet::new();
                    set.insert(token);

                    self.0.insert(event, set);
                }
            }
        }
    }

    pub fn unsubscribe(&mut self, token: Token, events: &Option<Vec<(Vec<u8>, usize)>>) {
        let Some(events) = events else {
            return;
        };

        for (event, _) in events {
            if let Some(set) = self.0.get_mut(event) {
                set.remove(&token);
                if set.is_empty() {
                    self.0.remove(event);
                }
            }
        }
    }

    pub fn publish(&self, event: Vec<u8>) -> Vec<Token> {
        match self.0.get(&event) {
            Some(clients) => clients.iter().cloned().collect(),
            None => Vec::new(),
        }
    }
}

impl Default for EventMap {
    fn default() -> Self {
        Self::new()
    }
}

impl EventMap {
    pub fn new() -> Self {
        EventMap(HashMap::new())
    }

    pub fn subscribe(&mut self, token: Token, events: Vec<Vec<u8>>) -> Vec<(Vec<u8>, usize)> {
        match self.0.get_mut(&token) {
            Some(set) => {
                let mut result = Vec::new();

                let mut count = set.len();

                for event in events {
                    if set.insert(event.clone()) {
                        count += 1;
                        result.push((event, count));
                    }
                }

                result
            }
            None => {
                let mut set = HashSet::new();
                let mut result = Vec::new();
                let mut count = 0;

                for event in events {
                    if set.insert(event.clone()) {
                        count += 1;
                        result.push((event, count));
                    }
                }

                self.0.insert(token, set);

                result
            }
        }
    }

    pub fn unsubscribe(
        &mut self,
        events: Vec<Vec<u8>>,
        token: Token,
        subscribed_clients: &mut HashSet<Token>,
    ) -> Option<Vec<(Vec<u8>, usize)>> {
        match self.0.get_mut(&token) {
            Some(existing_events) => {
                let mut result = Vec::new();
                let mut count = existing_events.len();

                if !events.is_empty() {
                    for event in events {
                        if existing_events.remove(&event) {
                            count -= 1;
                        }

                        result.push((event, count));
                    }

                    if existing_events.is_empty() {
                        self.0.remove(&token);
                        subscribed_clients.remove(&token);
                    }

                    Some(result)
                } else {
                    let unsubscribed_events: Vec<Vec<u8>> =
                        std::mem::take(existing_events).into_iter().collect();
                    let mut count = unsubscribed_events.len();

                    for event in unsubscribed_events {
                        count -= 1;
                        result.push((event, count));
                    }

                    subscribed_clients.remove(&token);
                    self.0.remove(&token);

                    Some(result)
                }
            }
            None => None,
        }
    }
}

pub struct Wish {
    token: Token,
    command_type: CommandType,
}

#[derive(Clone)]
pub enum CommandType {
    Server(ServerCommand),
    Client(ClientCommand),
}

#[derive(Clone)]
pub enum ServerCommand {
    Save {
        tx: Sender<Result<(), ServerError>>,
        file_path: PathBuf,
    },
    GetFilePath {
        tx: Sender<Result<Vec<u8>, ServerError>>,
    },
}

#[derive(Clone)]
pub struct ClientCommand {
    tx: Sender<Decree>,
    client_command_type: ClientCommandType,
}

#[derive(Clone)]
pub enum ClientCommandType {
    Database(DatabaseCommand),
    Broadcast(BroadcastCommand),
}

#[derive(Clone)]
pub enum BroadcastCommand {
    Subscribe { events: Vec<Vec<u8>> },
    Publish { event: Vec<u8>, message: Vec<u8> },
    Unsubscribe { terms: Vec<Vec<u8>> },
}

#[derive(Clone)]
pub enum DatabaseCommand {
    Get {
        key: Vec<u8>,
        time: u64,
    },
    Set {
        key: Vec<u8>,
        value: (Value, Option<u64>),
    },
    Del {
        keys: Vec<Vec<u8>>,
        time: u64,
    },
    Append {
        key: Vec<u8>,
        value: Vec<u8>,
        time: u64,
    },
    Incr {
        key: Vec<u8>,
        time: u64,
    },
    Incrby {
        key: Vec<u8>,
        number: i64,
        time: u64,
    },
    Decr {
        key: Vec<u8>,
        time: u64,
    },
    Strlen {
        key: Vec<u8>,
        time: u64,
    },
    Exists {
        keys: Vec<Vec<u8>>,
        time: u64,
    },
    Hset {
        key: Vec<u8>,
        field_value_pairs: Vec<(Vec<u8>, Vec<u8>)>,
        time: u64,
    },
    Hget {
        key: Vec<u8>,
        field: Vec<u8>,
        time: u64,
    },
    Hmget {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
        time: u64,
    },
    Hdel {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
        time: u64,
    },
    Hexists {
        key: Vec<u8>,
        field: Vec<u8>,
        time: u64,
    },
    Hlen {
        key: Vec<u8>,
        time: u64,
    },
    Lpush {
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
        time: u64,
    },
    Lpop {
        key: Vec<u8>,
        time: u64,
    },
    LpopM {
        key: Vec<u8>,
        count: usize,
        time: u64,
    },
    Rpush {
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
        time: u64,
    },
    Rpop {
        key: Vec<u8>,
        time: u64,
    },
    RpopM {
        key: Vec<u8>,
        count: usize,
        time: u64,
    },
    Llen {
        key: Vec<u8>,
        time: u64,
    },
    Lrange {
        key: Vec<u8>,
        starting_index: i32,
        ending_index: i32,
        time: u64,
    },
    Lindex {
        key: Vec<u8>,
        index: i32,
        time: u64,
    },
    Lset {
        key: Vec<u8>,
        index: i32,
        element: Vec<u8>,
        time: u64,
    },
    Lrem {
        key: Vec<u8>,
        count: i32,
        element: Vec<u8>,
        time: u64,
    },
    Expire {
        key: Vec<u8>,
        expiry: u64,
        time: u64,
    },
    Ttl {
        key: Vec<u8>,
        time: SystemTime,
    },
    Mset {
        terms_iter: IntoIter<Vec<u8>>,
    },
    Mget {
        terms_iter: IntoIter<Vec<u8>>,
        time: u64,
    },
    Sadd {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        time: u64,
    },
    Srem {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        time: u64,
    },
    Sismember {
        key: Vec<u8>,
        value: Vec<u8>,
        time: u64,
    },
    Hgetall {
        key: Vec<u8>,
        time: u64,
    },
    Smembers {
        key: Vec<u8>,
        time: u64,
    },
    ConfigGet {
        properties: Vec<Vec<u8>>,
    },
}

#[derive(Clone)]
pub struct Temple {
    tx: Sender<Wish>,
}

impl Temple {
    pub fn worship(
        dir: Vec<u8>,
        dbfilename: Vec<u8>,
        ipv4_address: Vec<u8>,
        port: Vec<u8>,
        io_threads: Vec<u8>,
        event_capacity: Vec<u8>,
        max_memory: Vec<u8>,
        append_only: Vec<u8>,
    ) -> Self {
        let (tx, rx): (Sender<Wish>, Receiver<Wish>) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let mut soul: Soul = (|| {
                let db_file_path = [dir.as_slice(), b"/", dbfilename.as_slice()].concat();

                let Ok(db_file_path) = std::str::from_utf8(&db_file_path) else {
                    println!("Couldn't load snapshot, failed to access file");
                    return Soul::new();
                };

                let Ok(bytes) = std::fs::read(db_file_path) else {
                    println!("Couldn't load snapshot, failed to read file");
                    return Soul::new();
                };

                let Ok(archived_soul) = rkyv::access::<ArchivedSoul, Error>(&bytes) else {
                    return Soul::new();
                };

                match deserialize::<_, Error>(archived_soul) {
                    Ok(snapshot) => {
                        println!("Snapshot loaded successfully");
                        snapshot
                    }
                    Err(e) => {
                        println!("Couldn't load snapshot: {}", e);
                        Soul::new()
                    }
                }
            })();

            let mut client_map = ClientMap::new();
            let mut event_map = EventMap::new();
            let mut subscribed_clients = HashSet::new();

            // let mut info: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut config: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

            // soul.set_info();
            Temple::initialize_config(
                &mut config,
                dir,
                dbfilename,
                ipv4_address,
                port,
                io_threads,
                event_capacity,
                max_memory,
                append_only,
            );

            loop {
                match rx.recv() {
                    Ok(wish) => {
                        let token = wish.token;

                        let command_type = wish.command_type;

                        match command_type {
                            Server(server_command) => match server_command {
                                Save { tx, file_path } => {
                                    if tx.send(soul.save(file_path)).is_err() {
                                        eprintln!(
                                            "Failed to send SAVE result: Temple channel closed"
                                        );
                                    }

                                    break;
                                }
                                GetFilePath { tx } => {
                                    let Some(dir) = config.get("dir".as_bytes()) else {
                                        if tx.send(Err(ServerError::ValueNotSet)).is_err() {
                                            eprintln!(
                                                "Failed to send command response: channel closed"
                                            );
                                        }

                                        return;
                                    };

                                    let Some(dbfilename) = config.get("dbfilename".as_bytes())
                                    else {
                                        if tx.send(Err(ServerError::ValueNotSet)).is_err() {
                                            eprintln!(
                                                "Failed to send command response: channel closed"
                                            );
                                        }

                                        return;
                                    };

                                    let mut file_path: Vec<u8> =
                                        Vec::with_capacity(dir.len() + dbfilename.len());
                                    file_path.extend(dir);
                                    file_path.push(b'/');
                                    file_path.extend(dbfilename);

                                    if tx.send(Ok(file_path)).is_err() {
                                        eprintln!(
                                            "Failed to send command response: channel closed"
                                        );
                                    }
                                }
                            },
                            Client(client_command) => {
                                let tx = client_command.tx;

                                match client_command.client_command_type {
                                    Broadcast(broadcast_command) => match broadcast_command {
                                        Subscribe { events } => {
                                            subscribed_clients.insert(token);

                                            let subscribed_channels =
                                                event_map.subscribe(token, events.clone());
                                            client_map.subscribe(token, events.clone());

                                            if tx
                                                .send(Decree::Deliver(Gift {
                                                    token,
                                                    response: Response::SubscribedChannels(
                                                        subscribed_channels,
                                                    ),
                                                }))
                                                .is_err()
                                            {
                                                eprintln!(
                                                    "Failed to send command response: channel closed"
                                                );
                                            }

                                            continue;
                                        }
                                        Unsubscribe { terms } => {
                                            let unsubscribed_events = event_map.unsubscribe(
                                                terms,
                                                token,
                                                &mut subscribed_clients,
                                            );
                                            client_map.unsubscribe(token, &unsubscribed_events);

                                            if tx
                                                .send(Decree::Deliver(Gift {
                                                    token,
                                                    response: Response::UnsubscribedChannels(
                                                        unsubscribed_events,
                                                    ),
                                                }))
                                                .is_err()
                                            {
                                                eprintln!(
                                                    "Failed to send command response: channel closed"
                                                );
                                            };

                                            continue;
                                        }
                                        Publish { event, message } => {
                                            let clients = client_map.publish(event.clone());

                                            if tx
                                                .send(Decree::Broadcast(
                                                    token, event, message, clients,
                                                ))
                                                .is_err()
                                            {
                                                eprintln!(
                                                    "Failed to send command response: channel closed"
                                                );
                                            }
                                        }
                                    },
                                    Database(database_command) => {
                                        if subscribed_clients.contains(&token) {
                                            if tx
                                                .send(Decree::Deliver(Gift {
                                                    token,
                                                    response: Response::Error(
                                                        Sacrilege::SubscriberOnlyMode,
                                                    ),
                                                }))
                                                .is_err()
                                            {
                                                eprintln!(
                                                    "Failed to send command response: channel closed"
                                                );
                                            }

                                            continue;
                                        }
                                        match database_command {
                                            Get { key, time } => match soul.get(key, time) {
                                                Ok(bulk_string) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::BulkString(
                                                                bulk_string,
                                                            ),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Set { key, value: val } => {
                                                soul.set(key, val);

                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::Info(InfoType::Ok),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Del { keys, time } => {
                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::Amount(
                                                            soul.del(keys, time),
                                                        ),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Append { key, value, time } => {
                                                match soul.append(key, value, time) {
                                                    Ok(length) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Length(length),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }

                                            Incr { key, time } => match soul.incr(key, time) {
                                                Ok(number) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Number(number),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Incrby { key, number, time } => match soul
                                                .incrby(key, number, time)
                                            {
                                                Ok(number) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Number(number),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Decr { key, time } => match soul.decr(key, time) {
                                                Ok(number) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Number(number),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Strlen { key, time } => match soul.strlen(key, time) {
                                                Ok(length) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Length(length),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Exists { keys, time } => {
                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::Amount(
                                                            soul.exists(keys, time),
                                                        ),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Hset {
                                                key,
                                                field_value_pairs,

                                                time,
                                            } => match soul.hset(key, field_value_pairs, time) {
                                                Ok(new_values_added) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Amount(
                                                                new_values_added,
                                                            ),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    };
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    };
                                                }
                                            },
                                            Hget { key, field, time } => {
                                                match soul.hget(key, field, time) {
                                                    Ok(bulk_string) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::BulkString(
                                                                    bulk_string,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Hmget { key, fields, time } => match soul
                                                .hmget(key, fields, time)
                                            {
                                                Ok(bulk_string_array) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::BulkStringArray(
                                                                bulk_string_array,
                                                            ),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Hdel { key, fields, time } => {
                                                match soul.hdel(key, fields, time) {
                                                    Ok(amount) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Amount(amount),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Hexists { key, field, time } => match soul
                                                .hexists(key, field, time)
                                            {
                                                Ok(amount) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Amount(amount),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Hlen { key, time } => match soul.hlen(key, time) {
                                                Ok(length) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Length(length),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Lpush {
                                                key,
                                                elements,

                                                time,
                                            } => match soul.lpush(key, elements, time) {
                                                Ok(length) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Length(length),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Lpop { key, time } => match soul.lpop(key, time) {
                                                Ok(element) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::BulkString(element),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            LpopM { key, count, time } => {
                                                match soul.lpop_m(key, count, time) {
                                                    Ok(elements) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::BulkStringArray(
                                                                    elements,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Rpush {
                                                key,
                                                elements,

                                                time,
                                            } => match soul.rpush(key, elements, time) {
                                                Ok(length) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Length(length),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Rpop { key, time } => match soul.rpop(key, time) {
                                                Ok(element) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::BulkString(element),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            RpopM { key, count, time } => {
                                                match soul.rpop_m(key, count, time) {
                                                    Ok(elements) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::BulkStringArray(
                                                                    elements,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Llen { key, time } => match soul.llen(key, time) {
                                                Ok(length) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Length(length),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Lrange {
                                                key,
                                                starting_index,
                                                ending_index,
                                                time,
                                            } => match soul.lrange(
                                                key,
                                                starting_index,
                                                ending_index,
                                                time,
                                            ) {
                                                Ok(bulk_string_array) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::BulkStringArray(
                                                                bulk_string_array,
                                                            ),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Lindex { key, index, time } => {
                                                match soul.lindex(key, index, time) {
                                                    Ok(element) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::BulkString(
                                                                    element,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Lset {
                                                key,
                                                element,
                                                index,

                                                time,
                                            } => match soul.lset(key, index, element, time) {
                                                Ok(_) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Info(InfoType::Ok),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Lrem {
                                                key,
                                                element,
                                                count,
                                                time,
                                            } => match soul.lrem(key, count, element, time) {
                                                Ok(amount) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Length(amount),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                                Err(sacrilege) => {
                                                    if tx
                                                        .send(Decree::Deliver(Gift {
                                                            token,
                                                            response: Response::Error(sacrilege),
                                                        }))
                                                        .is_err()
                                                    {
                                                        eprintln!(
                                                            "Failed to send command response: channel closed"
                                                        );
                                                    }
                                                }
                                            },
                                            Expire { key, expiry, time } => {
                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::Amount(
                                                            soul.expire(key, expiry, time),
                                                        ),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Ttl { key, time } => {
                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::Number(
                                                            soul.ttl(key, time),
                                                        ),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Mset { terms_iter } => {
                                                soul.mset(terms_iter);

                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::Info(InfoType::Ok),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Mget { terms_iter, time } => {
                                                let bulk_string_array = soul.mget(terms_iter, time);

                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::BulkStringArray(
                                                            bulk_string_array,
                                                        ),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                            Sadd { key, values, time } => {
                                                match soul.sadd(key, values, time) {
                                                    Ok(amount) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Length(amount),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Srem { key, values, time } => {
                                                match soul.srem(key, values, time) {
                                                    Ok(amount) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Length(amount),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Sismember { key, value, time } => {
                                                match soul.sismember(key, value, time) {
                                                    Ok(amount) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Length(amount),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Hgetall { key, time } => {
                                                match soul.hgetall(key, time) {
                                                    Ok(bulk_string_array) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::BulkStringArray(
                                                                    bulk_string_array,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            Smembers { key, time } => {
                                                match soul.smembers(key, time) {
                                                    Ok(bulk_string_array) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::BulkStringArray(
                                                                    bulk_string_array,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                    Err(sacrilege) => {
                                                        if tx
                                                            .send(Decree::Deliver(Gift {
                                                                token,
                                                                response: Response::Error(
                                                                    sacrilege,
                                                                ),
                                                            }))
                                                            .is_err()
                                                        {
                                                            eprintln!(
                                                                "Failed to send command response: channel closed"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                            ConfigGet { properties } => {
                                                let mut result = Vec::new();

                                                if properties.contains(&b"*".to_vec()) {
                                                    for (property, value) in config.iter() {
                                                        result.push(Some(property).cloned());
                                                        result.push(Some(value).cloned());
                                                    }

                                                    result.push(Some(b"databases".to_vec()));
                                                    result.push(Some(b"1".to_vec()));
                                                } else {
                                                    for (_, property) in
                                                        properties.iter().enumerate()
                                                    {
                                                        if let Some(value) = config.get(property) {
                                                            result.push(Some(property).cloned());
                                                            result.push(Some(value).cloned());
                                                        }
                                                    }

                                                    if properties.contains(&b"databases".to_vec()) {
                                                        result.push(Some(b"databases".to_vec()));
                                                        result.push(Some(b"1".to_vec()));
                                                    }
                                                }

                                                if tx
                                                    .send(Decree::Deliver(Gift {
                                                        token,
                                                        response: Response::BulkStringArray(Some(
                                                            result,
                                                        )),
                                                    }))
                                                    .is_err()
                                                {
                                                    eprintln!(
                                                        "Failed to send command response: channel closed"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("GodThread: {}", e);
                        break;
                    }
                }
            }
        });

        Temple { tx }
    }

    pub fn get(&self, key: Vec<u8>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Get { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn set(&self, key: Vec<u8>, value: (Value, Option<u64>), tx: Sender<Decree>, token: Token) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Set { key, value }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn del(&self, keys: Vec<Vec<u8>>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Del { keys, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn exists(&self, keys: Vec<Vec<u8>>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Exists { keys, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn append(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        tx: Sender<Decree>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Append { key, value, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn incr(&self, key: Vec<u8>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Incr { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn incrby(&self, key: Vec<u8>, number: i64, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(DatabaseCommand::Incrby { key, number, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn decr(&self, key: Vec<u8>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Decr { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn strlen(&self, key: Vec<u8>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Strlen { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hset(
        &self,
        key: Vec<u8>,
        field_value_pairs: Vec<(Vec<u8>, Vec<u8>)>,
        tx: Sender<Decree>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hset {
                        key,
                        field_value_pairs,
                        time,
                    }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hget(&self, tx: Sender<Decree>, key: Vec<u8>, field: Vec<u8>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hget { key, field, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hmget(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hmget { key, fields, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hdel(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hdel { key, fields, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hexists(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        field: Vec<u8>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hexists { key, field, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hlen(&self, tx: Sender<Decree>, key: Vec<u8>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hlen { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lpush(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Lpush {
                        key,
                        elements,
                        time,
                    }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lpop(&self, tx: Sender<Decree>, key: Vec<u8>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Lpop { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lpop_m(&self, tx: Sender<Decree>, key: Vec<u8>, count: usize, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(LpopM { key, count, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn rpush(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Rpush {
                        key,
                        elements,
                        time,
                    }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn rpop(&self, tx: Sender<Decree>, key: Vec<u8>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Rpop { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn rpop_m(&self, tx: Sender<Decree>, key: Vec<u8>, count: usize, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(RpopM { key, count, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn llen(&self, tx: Sender<Decree>, key: Vec<u8>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Llen { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lrange(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        starting_index: i32,
        ending_index: i32,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Lrange {
                        key,
                        starting_index,
                        ending_index,
                        time,
                    }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lindex(&self, tx: Sender<Decree>, key: Vec<u8>, index: i32, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Lindex { key, index, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lset(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        index: i32,
        element: Vec<u8>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Lset {
                        key,
                        index,
                        element,
                        time,
                    }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn lrem(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        count: i32,
        element: Vec<u8>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Lrem {
                        key,
                        count,
                        element,
                        time,
                    }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn expire(&self, tx: Sender<Decree>, key: Vec<u8>, expiry: u64, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Expire { key, expiry, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn ttl(&self, tx: Sender<Decree>, key: Vec<u8>, token: Token, time: SystemTime) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Ttl { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn mset(&self, terms_iter: IntoIter<Vec<u8>>, tx: Sender<Decree>, token: Token) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Mset { terms_iter }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn mget(&self, terms_iter: IntoIter<Vec<u8>>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Mget { terms_iter, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn sadd(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Sadd { key, values, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn srem(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Srem { key, values, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn sismember(
        &self,
        tx: Sender<Decree>,
        key: Vec<u8>,
        value: Vec<u8>,
        token: Token,
        time: u64,
    ) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Sismember { key, value, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn hgetall(&self, key: Vec<u8>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Hgetall { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn smembers(&self, key: Vec<u8>, tx: Sender<Decree>, token: Token, time: u64) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(Smembers { key, time }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn subscribe(&self, tx: Sender<Decree>, events: Vec<Vec<u8>>, token: Token) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Broadcast(Subscribe { events }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn publish(&self, tx: Sender<Decree>, event: Vec<u8>, message: Vec<u8>, token: Token) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Broadcast(Publish { event, message }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn unsubscribe(&self, tx: Sender<Decree>, token: Token, terms: Vec<Vec<u8>>) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Broadcast(Unsubscribe { terms }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn config_get(&self, tx: Sender<Decree>, token: Token, properties: Vec<Vec<u8>>) {
        if self
            .tx
            .send(Wish {
                token,
                command_type: CommandType::Client(ClientCommand {
                    tx,
                    client_command_type: Database(ConfigGet { properties }),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    pub fn save(&mut self, tx: Sender<Result<(), ServerError>>, token: Token) {
        let (server_tx, server_rx) = std::sync::mpsc::channel();

        if self
            .tx
            .send(Wish {
                token,
                command_type: Server(ServerCommand::GetFilePath { tx: server_tx }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        let file_path = match server_rx.recv() {
            Ok(Ok(file_path)) => {
                PathBuf::from(std::str::from_utf8(&file_path).expect("File path Invalid"))
            }
            _ => {
                eprintln!("Couldn't get file path");
                return;
            }
        };

        if self
            .tx
            .send(Wish {
                token,
                command_type: Server(Save {
                    tx,
                    file_path: file_path.to_path_buf(),
                }),
            })
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    }

    fn initialize_config(
        config: &mut HashMap<Vec<u8>, Vec<u8>>,
        dir: Vec<u8>,
        dbfilename: Vec<u8>,
        ipv4_address: Vec<u8>,
        port: Vec<u8>,
        io_threads: Vec<u8>,
        event_capacity: Vec<u8>,
        max_memory: Vec<u8>,
        append_only: Vec<u8>,
    ) {
        config.insert("dir".as_bytes().to_vec(), dir);
        config.insert("dbfilename".as_bytes().to_vec(), dbfilename);
        config.insert("ipv4_address".as_bytes().to_vec(), ipv4_address);
        config.insert("port".as_bytes().to_vec(), port);
        config.insert("io_threads".as_bytes().to_vec(), io_threads);
        config.insert("max_memory".as_bytes().to_vec(), max_memory);
        config.insert("event_capacity".as_bytes().to_vec(), event_capacity);
        config.insert("append_only".as_bytes().to_vec(), append_only);
    }

    pub fn sanctify(&self) -> Self {
        self.clone()
    }
}

==> ./src/temple/soul.rs <==
use std::collections::hash_map::Entry;
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use std::vec::IntoIter;
use std::{collections::HashMap, time::SystemTime};

use rkyv::rancor::Error;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wish::util::bytes_to_i64;
use crate::wish::{Command, Sacrilege};

#[derive(Clone, Archive, Serialize, Deserialize)]
pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
}

#[derive(Archive, Serialize, Deserialize)]
pub struct Soul(HashMap<Vec<u8>, (Value, Option<u64>)>);

pub enum ServerError {
    SerializationError(String),
    FileWriteError(String),
    ValueNotSet,
}

impl Default for Soul {
    fn default() -> Self {
        Self::new()
    }
}

use ServerError::{FileWriteError, SerializationError};

impl Soul {
    pub fn new() -> Self {
        Soul(HashMap::new())
    }

    pub fn save(&self, path: PathBuf) -> Result<(), ServerError> {
        let bytes = match rkyv::to_bytes::<Error>(self) {
            Ok(bytes) => bytes,
            Err(err) => return Err(SerializationError(err.to_string())),
        };

        if let Err(e) = std::fs::write(path, bytes) {
            return Err(FileWriteError(e.to_string()));
        }

        Ok(())
    }

    pub fn get(&mut self, key: Vec<u8>, now: u64) -> Result<Option<Vec<u8>>, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::GET)),
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, val: (Value, Option<u64>)) {
        self.0.insert(key, val);
    }

    pub fn append(
        &mut self,
        key: Vec<u8>,
        mut incoming_value: Vec<u8>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::String(value)) => {
                value.append(&mut incoming_value);
                Ok(value.len())
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::APPEND)),
            None => {
                let incoming_value_len = incoming_value.len();
                self.0.insert(key, (Value::String(incoming_value), None));
                Ok(incoming_value_len)
            }
        }
    }

    pub fn incr(&mut self, key: Vec<u8>, now: u64) -> Result<i64, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::String(value)) => {
                let mut itoa_buf = itoa::Buffer::new();

                let Ok(number) = bytes_to_i64(value) else {
                    return Err(Sacrilege::IncorrectUsage(Command::INCR));
                };

                let number = number
                    .checked_add(1)
                    .ok_or(Sacrilege::IncorrectUsage(Command::INCR))?;

                value.clear();
                value.extend_from_slice(itoa_buf.format(number).as_bytes());

                Ok(number)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::INCR)),
            None => {
                self.0.insert(key, (Value::String(b"1".into()), None));
                Ok(1)
            }
        }
    }

    pub fn incrby(
        &mut self,
        key: Vec<u8>,
        number_to_increment_by: i64,
        now: u64,
    ) -> Result<i64, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::String(value)) => {
                let mut itoa_buf = itoa::Buffer::new();

                let Ok(number) = bytes_to_i64(value) else {
                    return Err(Sacrilege::IncorrectUsage(Command::INCRBY));
                };

                let number = number
                    .checked_add(number_to_increment_by)
                    .ok_or(Sacrilege::IncorrectUsage(Command::INCRBY))?;

                value.clear();
                value.extend_from_slice(itoa_buf.format(number).as_bytes());

                Ok(number)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::INCRBY)),
            None => {
                let mut itoa_buf = itoa::Buffer::new();

                self.0.insert(
                    key,
                    (
                        Value::String(itoa_buf.format(number_to_increment_by).into()),
                        None,
                    ),
                );
                Ok(number_to_increment_by)
            }
        }
    }

    pub fn decr(&mut self, key: Vec<u8>, now: u64) -> Result<i64, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::String(value)) => {
                let mut itoa_buf = itoa::Buffer::new();

                let Ok(number) = bytes_to_i64(value) else {
                    return Err(Sacrilege::IncorrectUsage(Command::DECR));
                };

                let number = number
                    .checked_add(-1)
                    .ok_or(Sacrilege::IncorrectUsage(Command::DECR))?;

                value.clear();
                value.extend_from_slice(itoa_buf.format(number).as_bytes());

                Ok(number)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::DECR)),
            None => {
                self.0.insert(key, (Value::String(b"-1".into()), None));
                Ok(-1)
            }
        }
    }

    pub fn strlen(&mut self, key: Vec<u8>, now: u64) -> Result<usize, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::String(value)) => Ok(value.len()),
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::STRLEN)),
            None => Ok(0),
        }
    }

    pub fn del(&mut self, keys: Vec<Vec<u8>>, now: u64) -> u32 {
        let mut number_of_entries_deleted = 0;

        for key in keys {
            if self.remove_valid_value(&key, now).is_some() {
                number_of_entries_deleted += 1;
            }
        }

        number_of_entries_deleted
    }

    pub fn exists(&mut self, keys: Vec<Vec<u8>>, now: u64) -> u32 {
        let mut number_of_entries_that_exist = 0;

        for key in keys {
            if self.get_valid_value(&key, now).is_some() {
                number_of_entries_that_exist += 1;
            }
        }

        number_of_entries_that_exist
    }

    pub fn hset(
        &mut self,
        key: Vec<u8>,
        field_value_pairs: Vec<(Vec<u8>, Vec<u8>)>,
        now: u64,
    ) -> Result<u32, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::Hash(map)) => {
                let mut new_values_added = 0;

                for field_value_pair in field_value_pairs {
                    let (field, value) = field_value_pair;

                    if map.insert(field, value).is_none() {
                        new_values_added += 1;
                    }
                }

                Ok(new_values_added)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HSET)),
            None => {
                let mut map = HashMap::new();
                let mut new_values_added = 0;

                for field_value_pair in field_value_pairs {
                    let (field, value) = field_value_pair;

                    map.insert(field, value);
                    new_values_added += 1;
                }

                self.0.insert(key, (Value::Hash(map), None));

                Ok(new_values_added)
            }
        }
    }

    pub fn hget(
        &mut self,
        key: Vec<u8>,
        field: Vec<u8>,
        now: u64,
    ) -> Result<Option<Vec<u8>>, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::Hash(map)) => Ok(map.get(&field).cloned()),
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HGET)),
            None => Ok(None),
        }
    }

    pub fn hmget(
        &mut self,
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
        now: u64,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>, Sacrilege> {
        let mut values = Vec::new();

        match self.get_valid_value(&key, now) {
            Some(Value::Hash(map)) => {
                for field in fields {
                    values.push(map.get(&field).cloned());
                }

                Ok(Some(values))
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HMGET)),
            None => Ok(None),
        }
    }

    pub fn hdel(&mut self, key: Vec<u8>, fields: Vec<Vec<u8>>, now: u64) -> Result<u32, Sacrilege> {
        let mut amount_of_deleted_values = 0;

        match self.get_mut_valid_value(&key, now) {
            Some(Value::Hash(map)) => {
                for field in fields {
                    if map.remove(&field).is_some() {
                        amount_of_deleted_values += 1
                    }
                }

                Ok(amount_of_deleted_values)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HDEL)),
            None => Ok(0),
        }
    }

    pub fn hexists(&mut self, key: Vec<u8>, field: Vec<u8>, now: u64) -> Result<u32, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::Hash(map)) => {
                if map.get(&field).is_some() {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HEXISTS)),
            None => Ok(0),
        }
    }

    pub fn hlen(&mut self, key: Vec<u8>, now: u64) -> Result<usize, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::Hash(map)) => Ok(map.len()),
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HLEN)),
            None => Ok(0),
        }
    }

    pub fn lpush(
        &mut self,
        key: Vec<u8>,
        mut elements: Vec<Vec<u8>>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::List(list)) => {
                for element in elements {
                    list.push_front(element);
                }
                Ok(list.len())
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::LPUSH)),
            None => {
                let elements_len = elements.len();
                elements.reverse();

                self.0
                    .insert(key, (Value::List(VecDeque::from(elements)), None));

                Ok(elements_len)
            }
        }
    }

    pub fn lpop(&mut self, key: Vec<u8>, now: u64) -> Result<Option<Vec<u8>>, Sacrilege> {
        match self.0.entry(key) {
            Entry::Occupied(mut occupied) => {
                if let (Value::List(list), expiry) = occupied.get_mut() {
                    if let Some(expiry) = expiry
                        && *expiry < now
                    {
                        occupied.remove();
                        return Ok(None);
                    }

                    let element = list.pop_front();

                    if list.is_empty() {
                        occupied.remove();
                    }

                    Ok(element)
                } else {
                    Err(Sacrilege::IncorrectUsage(Command::LPOP))
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn lpop_m(
        &mut self,
        key: Vec<u8>,
        count: usize,
        now: u64,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>, Sacrilege> {
        match self.0.entry(key) {
            Entry::Occupied(mut occupied) => {
                if let (Value::List(list), expiry) = occupied.get_mut() {
                    if let Some(expiry) = expiry
                        && *expiry < now
                    {
                        occupied.remove();
                        return Ok(None);
                    }

                    let mut popped = Vec::new();

                    for _ in 0..count {
                        if let Some(element) = list.pop_front() {
                            popped.push(Some(element));
                        } else {
                            break;
                        }
                    }

                    if list.is_empty() {
                        occupied.remove();
                    }

                    Ok(Some(popped))
                } else {
                    Err(Sacrilege::IncorrectUsage(Command::LPOP))
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn rpush(
        &mut self,
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::List(list)) => {
                for element in elements {
                    list.push_back(element);
                }
                Ok(list.len())
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::RPUSH)),
            None => {
                let elements_len = elements.len();

                self.0
                    .insert(key, (Value::List(VecDeque::from(elements)), None));

                Ok(elements_len)
            }
        }
    }

    pub fn rpop(&mut self, key: Vec<u8>, now: u64) -> Result<Option<Vec<u8>>, Sacrilege> {
        match self.0.entry(key) {
            Entry::Occupied(mut occupied) => {
                if let (Value::List(list), expiry) = occupied.get_mut() {
                    if let Some(expiry) = expiry
                        && *expiry < now
                    {
                        occupied.remove();
                        return Ok(None);
                    }

                    let element = list.pop_back();

                    if list.is_empty() {
                        occupied.remove();
                    }

                    Ok(element)
                } else {
                    Err(Sacrilege::IncorrectUsage(Command::RPOP))
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn rpop_m(
        &mut self,
        key: Vec<u8>,
        count: usize,
        now: u64,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>, Sacrilege> {
        match self.0.entry(key) {
            Entry::Occupied(mut occupied) => {
                if let (Value::List(list), expiry) = occupied.get_mut() {
                    if let Some(expiry) = expiry
                        && *expiry < now
                    {
                        occupied.remove();
                        return Ok(None);
                    }

                    let mut popped = Vec::new();

                    for _ in 0..count {
                        if let Some(element) = list.pop_back() {
                            popped.push(Some(element));
                        } else {
                            break;
                        }
                    }

                    if list.is_empty() {
                        occupied.remove();
                    }

                    Ok(Some(popped))
                } else {
                    Err(Sacrilege::IncorrectUsage(Command::RPOP))
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    pub fn llen(&mut self, key: Vec<u8>, now: u64) -> Result<usize, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::List(list)) => Ok(list.len()),
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::LLEN)),
            None => Ok(0),
        }
    }

    pub fn lrange(
        &mut self,
        key: Vec<u8>,
        mut starting_index: i32,
        mut ending_index: i32,
        now: u64,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::List(list)) => {
                let list_len = list.len() as i32;

                if starting_index < 0 {
                    starting_index += list_len;
                }

                if ending_index < 0 {
                    ending_index += list_len;
                }

                if starting_index < 0 {
                    starting_index = 0;
                }

                if ending_index > list_len {
                    ending_index = list_len - 1;
                }

                if ending_index - starting_index >= 0
                    && starting_index < list_len
                    && ending_index < list_len
                {
                    Ok(Some(
                        list.range(starting_index as usize..(ending_index + 1) as usize)
                            .map(|e| Some(e.clone()))
                            .collect(),
                    ))
                } else {
                    Ok(vec![].into())
                }
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::LRANGE)),
            None => Ok(vec![].into()),
        }
    }

    pub fn lindex(
        &mut self,
        key: Vec<u8>,
        mut index: i32,
        now: u64,
    ) -> Result<Option<Vec<u8>>, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::List(list)) => {
                let list_len = list.len() as i32;

                if index < 0 {
                    index += list_len;
                }

                if index < 0 || index >= list_len {
                    return Ok(None);
                }

                Ok(list.get(index as usize).cloned())
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::LINDEX)),
            None => Ok(None),
        }
    }

    pub fn lset(
        &mut self,
        key: Vec<u8>,
        mut index: i32,
        element: Vec<u8>,
        now: u64,
    ) -> Result<(), Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::List(list)) => {
                let list_len = list.len() as i32;

                if index < 0 {
                    index += list_len;
                }

                if index < 0 || index >= list_len {
                    return Err(Sacrilege::IncorrectUsage(Command::LSET));
                }

                list[index as usize] = element;

                Ok(())
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::LSET)),
            None => Err(Sacrilege::IncorrectUsage(Command::LSET)),
        }
    }

    pub fn lrem(
        &mut self,
        key: Vec<u8>,
        mut count: i32,
        element: Vec<u8>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::List(list)) => {
                let initial_len = list.len();

                if count < 0 {
                    let mut idx: i32 = list.len() as i32 - 1;

                    while idx >= 0 && count < 0 {
                        if list[idx as usize] == element {
                            list.remove(idx as usize);
                            count += 1;
                        }

                        idx -= 1;
                    }
                } else if count > 0 {
                    let mut list_len = list.len();
                    let mut idx = 0;

                    while idx < list_len && count > 0 {
                        if list[idx] == element {
                            list.remove(idx);
                            count -= 1;
                            list_len -= 1;

                            continue;
                        }

                        idx += 1;
                    }
                } else {
                    list.retain(|existing_element| *existing_element != element);
                }

                Ok(initial_len - list.len())
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::LREM)),
            None => Ok(0),
        }
    }

    pub fn expire(&mut self, key: Vec<u8>, expiry: u64, now: u64) -> u32 {
        match self.0.entry(key) {
            Entry::Occupied(mut occupied) => {
                let (_, existing_expiry) = occupied.get_mut();

                if let Some(expiry) = existing_expiry
                    && *expiry < now
                {
                    occupied.remove();
                    return 0;
                }

                *existing_expiry = Some(expiry);
                1
            }
            Entry::Vacant(_) => 0,
        }
    }

    pub fn ttl(&mut self, key: Vec<u8>, now: SystemTime) -> i64 {
        match self.0.entry(key) {
            Entry::Occupied(mut occupied) => {
                let (_, existing_expiry) = occupied.get_mut();

                if let Some(expiry) = existing_expiry {
                    let expiry = UNIX_EPOCH + std::time::Duration::from_secs(*expiry);

                    if expiry < now {
                        occupied.remove();
                        -2
                    } else {
                        let Ok(duration) = expiry.duration_since(now) else {
                            occupied.remove();
                            return -2;
                        };

                        duration.as_secs() as i64
                    }
                } else {
                    -1
                }
            }
            Entry::Vacant(_) => -2,
        }
    }

    pub fn mset(&mut self, mut terms_iter: IntoIter<Vec<u8>>) {
        while let (Some(key), Some(value)) = (terms_iter.next(), terms_iter.next()) {
            self.0.insert(key, (Value::String(value), None));
        }
    }

    pub fn mget(
        &mut self,
        terms_iter: IntoIter<Vec<u8>>,
        now: u64,
    ) -> Option<Vec<Option<Vec<u8>>>> {
        let mut result = Vec::with_capacity(terms_iter.len());

        for key in terms_iter {
            match self.get_valid_value(&key, now) {
                Some(Value::String(value)) => {
                    result.push(Some(value.clone()));
                }
                _ => result.push(None),
            }
        }

        Some(result)
    }

    pub fn sadd(
        &mut self,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::Set(set)) => {
                let mut count = 0;

                for value in values {
                    if set.insert(value) {
                        count += 1;
                    }
                }

                Ok(count)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::SADD)),
            None => {
                let mut set = HashSet::new();
                let mut count = 0;

                for value in values {
                    if set.insert(value) {
                        count += 1;
                    }
                }

                self.0.insert(key, (Value::Set(set), None));

                Ok(count)
            }
        }
    }

    pub fn srem(
        &mut self,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_mut_valid_value(&key, now) {
            Some(Value::Set(set)) => {
                let mut count = 0;

                for value in values {
                    if set.remove(&value) {
                        count += 1;
                    }
                }

                if set.is_empty() {
                    self.0.remove(&key);
                }

                Ok(count)
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::SREM)),
            None => Ok(0),
        }
    }

    pub fn sismember(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        now: u64,
    ) -> Result<usize, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::Set(set)) => {
                if set.contains(&value) {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::SISMEMBER)),
            None => Ok(0),
        }
    }

    pub fn hgetall(
        &mut self,
        key: Vec<u8>,
        now: u64,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::Hash(map)) => {
                let mut result = Vec::with_capacity(map.len() * 2);

                for (field, value) in map {
                    result.push(Some(field.clone()));
                    result.push(Some(value.clone()));
                }

                Ok(Some(result))
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::HGETALL)),
            None => Ok(None),
        }
    }

    pub fn smembers(
        &mut self,
        key: Vec<u8>,
        now: u64,
    ) -> Result<Option<Vec<Option<Vec<u8>>>>, Sacrilege> {
        match self.get_valid_value(&key, now) {
            Some(Value::Set(set)) => {
                let mut result = Vec::with_capacity(set.len());

                for value in set {
                    result.push(Some(value.clone()));
                }

                Ok(Some(result))
            }
            Some(_) => Err(Sacrilege::IncorrectUsage(Command::SMEMBERS)),
            None => Ok(None),
        }
    }

    fn get_valid_value(&mut self, key: &Vec<u8>, now: u64) -> Option<&Value> {
        let is_expired = match self.0.get(key) {
            Some((_, Some(expiry))) => *expiry < now,
            _ => false,
        };

        if is_expired {
            self.0.remove(key);
            return None;
        } else {
            return self.0.get(key).map(|(value, _)| value);
        }
    }

    fn get_mut_valid_value(&mut self, key: &Vec<u8>, now: u64) -> Option<&mut Value> {
        let is_expired = match self.0.get(key) {
            Some((_, Some(expiry))) => *expiry < now,
            _ => false,
        };

        if is_expired {
            self.0.remove(key);
            return None;
        } else {
            return self.0.get_mut(key).map(|(value, _)| value);
        }
    }

    pub fn remove_valid_value(&mut self, key: &Vec<u8>, now: u64) -> Option<Value> {
        match self.0.remove(key) {
            Some((value, Some(expiry))) => {
                if expiry < now {
                    None
                } else {
                    Some(value)
                }
            }
            Some((value, None)) => Some(value),
            None => None,
        }
    }
}

==> ./src/egress.rs <==
use crate::wish::grant::Decree;
use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::{Receiver, Sender};

use mio::Token;

mod send;

pub fn egress(pilgrim_rx: Receiver<Decree>, egress_tx: Sender<Token>) {
    let mut egress_map: HashMap<Token, mio::net::TcpStream> = HashMap::new();
    let mut buffer = Vec::with_capacity(2100);
    let mut itoa_buf = itoa::Buffer::new();

    loop {
        match pilgrim_rx.recv() {
            Ok(Decree::Welcome(token, stream)) => {
                egress_map.insert(token, stream);
            }
            Ok(Decree::Deliver(gift)) => {
                if let Some(stream) = egress_map.get_mut(&gift.token) {
                    let token = gift.token;

                    if send::send(stream, gift, &mut buffer).is_err()
                        && egress_tx.send(token).is_err()
                    {
                        eprintln!("Failed to send command response: channel closed");
                    };
                }
            }
            Ok(Decree::Broadcast(token, event, message, clients)) => {
                let clients_len = clients.len();

                let mut response = b"*3\r\n$7\r\nmessage\r\n$".to_vec();
                response.extend_from_slice(itoa_buf.format(event.len()).as_bytes());
                response.extend_from_slice(b"\r\n");
                response.extend_from_slice(&event);
                response.extend_from_slice(b"\r\n$");
                response.extend_from_slice(itoa_buf.format(message.len()).as_bytes());
                response.extend_from_slice(b"\r\n");
                response.extend_from_slice(&message);
                response.extend_from_slice(b"\r\n");

                for client in clients {
                    if let Some(stream) = egress_map.get_mut(&client)
                        && stream.write_all(&response).is_err()
                    {
                        eprintln!("writing to stream failed for client");
                    }
                }

                if let Some(publisher_stream) = egress_map.get_mut(&token) {
                    let mut response = b":".to_vec();
                    response.extend_from_slice(itoa_buf.format(clients_len).as_bytes());
                    response.extend_from_slice(b"\r\n");

                    if publisher_stream.write_all(&response).is_err() {
                        eprintln!("writing to stream failed for publisher");
                    }
                }
            }
            Err(_) => break,
        }
    }
}

==> ./src/main.rs <==
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use clap::Parser;
use std::path::PathBuf;

mod server;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long = "bind", default_value = "0.0.0.0")]
    ip: String,

    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long = "io-threads", default_value_t = 3)]
    io_threads: usize,

    #[arg(long = "event-limit", default_value_t = 128)]
    event_limit: usize,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,
}

fn main() {
    let args = Args::parse();

    let dir = args
        .dir
        .unwrap_or_else(|| std::env::current_dir().expect("Couldn't access current directory"));

    if let Err(e) = server::run(
        &args.ip,
        args.port,
        args.io_threads,
        args.event_limit,
        dir,
        &args.dbfilename,
        0,
        "no",
    ) {
        eprintln!("Server failed to start: {}", e);
        std::process::exit(1);
    }
}

==> ./src/choir.rs <==
use crossbeam_channel::{Receiver, Sender, unbounded};

type Song = Box<dyn FnOnce() + Send + 'static>;

struct Angel {
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Angel {
    fn new(rx: Receiver<Song>) -> Self {
        Angel {
            thread: Some(std::thread::spawn(move || {
                while let Ok(song) = rx.recv() {
                    song();
                }
            })),
        }
    }
}

pub struct Choir {
    angels: Vec<Angel>,
    tx: Option<Sender<Song>>,
}

impl Choir {
    pub fn new(capacity: usize) -> Self {
        let mut angels = Vec::with_capacity(capacity);
        let (tx, rx) = unbounded();

        for _ in 0..capacity {
            angels.push(Angel::new(rx.clone()));
        }

        Choir {
            angels,
            tx: Some(tx),
        }
    }

    pub fn sing<F>(&self, song: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(tx) = &self.tx {
            tx.send(Box::new(song)).unwrap();
        }
    }
}

impl Drop for Choir {
    fn drop(&mut self) {
        drop(self.tx.take());

        for angel in &mut self.angels {
            angel.thread.take().unwrap().join().unwrap();
        }
    }
}

==> ./src/tests/pubsub_test.rs <==
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

==> ./src/tests/soul_test.rs <==
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
