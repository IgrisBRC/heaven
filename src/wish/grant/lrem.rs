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
        util::bytes_to_i32,
    },
};

pub fn lrem(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 4 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let (Some(key), Some(count), Some(element)) =
        (terms_iter.next(), terms_iter.next(), terms_iter.next())
    else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    };

    let Ok(index) = bytes_to_i32(&count) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::LREM)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return;
    };

    temple.lrem(
        tx,
        key,
        index,
        element,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );
}
