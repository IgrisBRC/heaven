use mio::Token;

use crate::{
    temple::Temple,
    wish::{Response, Sacrilege},
};

use std::sync::mpsc::Sender;

mod append;
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
mod command;
mod hlen;
mod hmget;
mod hset;
mod incr;
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
