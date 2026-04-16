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
