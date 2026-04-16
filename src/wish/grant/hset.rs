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
