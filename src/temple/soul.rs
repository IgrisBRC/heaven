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
