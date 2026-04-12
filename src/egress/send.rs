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
                Command::INCR | Command::DECR => {
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
