# Heaven 

This project is still in beta, and is not adviced to be used in production. 

Keynote: There is no max memory limit, the DB will keep growing untill OOM.


Heaven is an RESP2-compliant in-memory database completely written in safe Rust.

It does not support all the commands, but as for the commands that are supported, it is a drop-in replacement for redis.

## What it supports right now

Note: Every command is supposed to work just like how it would in Redis.

| Command | Category | Usage | Description |
| :--- | :--- | :--- | :--- |
| **SET** | String | `SET key value [EX seconds]` | Stores a string; supports optional TTL |
| **GET** | String | `GET key` | Retrieves a value; handles passive expiry |
| **APPEND** | String | `APPEND key value` | Appends to a key; acts like SET |
| **INCR / DECR** | String | `INCR key` | Atomic integer math on string values |
| **STRLEN** | String | `STRLEN key` | Retrieves length of string |
| **LPUSH / RPUSH**| List | `LPUSH key val [val...]` | Pushes to the Front (Left) or Back (Right) |
| **LPOP / RPOP** | List | `LPOP key [count]` | Pops element(s) from the Front or Back |
| **LRANGE** | List | `LRANGE key start stop` | Returns a slice of the list |
| **LREM** | List | `LREM key count element` | Removes elements based on directional search |
| **LINDEX** | List | `LINDEX key index` | Retrieves one element at given index in a list |
| **LLEN** | List | `LLEN key` | Retrieves number of elements in a list|
| **LSET** | List | `LSET key index element` | Replaces an element at a specific index |
| **HSET** | Hash | `HSET key field val [val ...]` | Sets fields within a hash map |
| **HMGET** | Hash | `HMGET key field [field ...]` | Gets fields within a hash map |
| **HGET** | Hash | `HGET key field` | Gets fields within a hash map |
| **HDEL** | Hash | `HDEL key field...` | Removes one or more fields from a hashmap |
| **HEXISTS** | Hash | `HEXISTS key field` | Checks for one field in a hashmap |
| **HLEN** | Hash | `HLEN key` | Retrieves the hashmap's size |
| **HGETALL** | Hash | `HGETALL key` | Returns all field value pairs in a hashmap at key |
| **SADD** | Set | `SADD key val [val ...]` | Addes value to a (new) set at key |
| **SREM** | Set | `SREM key val [val ...]` | Removes value from a set at key |
| **SISMEMBER** | Set | `SISMEMBER key val` | Checks if value is in a set at key |
| **SMEMBERS** | Set | `SMEMBERS key` | Returns all the values in a set at key |
| **EXISTS** | Generic | `EXISTS key [key...]` | Checks for the presence of keys |
| **DEL** | Generic | `DEL key [key...]` | Removes keys of any data type |
| **TTL** | Generic | `TTL key` | Returns the expiry of the entry at key|
| **EXPIRE** | Generic | `EXPIRE key expiry` | Sets the expiry of the entry at key |
| **SUBSCRIBE** | Broadcast | `SUBSCRIBE event [event ...]` | Subscribes you to event(s) |
| **UNSUBSCRIBE** | Broadcast | `UNSUBSCRIBE event [event ...]` | Unsubscribes you from the event(s) |
| **PUBLISH** | Broadcast | `PUBLISH event message` | Sends a message to all the clients subscribed to event |
| **PING** | System | `PING` | Returns `PONG` |
| **COMMAND** | System | `COMMAND` | Returns information about all the commands supported |


## Support for snapshots

As it stands Jerusalem only supports .rdb style snapshots using rkyv. AOF is not yet supported.

### A Note on PING

Currently, Heaven requires the standard RESP array protocol format for all commands. Some clients may attempt to send a "naked" PING during pipelining without the array marker (`*`). This is currently not supported to keep the parser logic clean and focused on standard protocol adherence.

## Usage

```bash
cargo r --release
```

Note: If you want to handle more than 508 concurrent connections, you may have to set ulimit to a higher number than 1024.

## Crates used

https://crates.io/crates/mio

https://crates.io/crates/crossbeam

https://crates.io/crates/jemallocator

https://crates.io/crates/itoa

https://crates.io/crates/rkyv

https://crates.io/crates/ctrlc

## Future plans

std::bytes
Io_uring
Metacommands
Sharding

