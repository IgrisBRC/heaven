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
        util::bytes_to_u64,
    },
};

pub fn expire(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::EXPIRE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    let (Some(key), Some(expiry)) = (terms_iter.next(), terms_iter.next()) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::EXPIRE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    let Ok(expiry) = bytes_to_u64(&expiry) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::EXPIRE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    temple.expire(
        tx,
        key,
        now + expiry,
        token,
        now,
    );
}
