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
