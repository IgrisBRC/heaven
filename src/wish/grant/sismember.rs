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
