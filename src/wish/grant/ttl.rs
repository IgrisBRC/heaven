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
