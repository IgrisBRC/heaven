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
