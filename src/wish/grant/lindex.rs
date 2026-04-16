use std::{sync::mpsc::Sender, time::{SystemTime, UNIX_EPOCH}};

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_i32,
    },
};

pub fn lindex(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3
        && tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LINDEX)),
            }))
            .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(index)) = (terms_iter.next(), terms_iter.next()) {
        if let Ok(index) = bytes_to_i32(&index) {
            temple.lindex(
                tx,
                key,
                index,
                token,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            );
        } else if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::LINDEX)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::LINDEX)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}
