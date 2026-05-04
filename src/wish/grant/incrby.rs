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
        util::bytes_to_i64,
    },
};

pub fn incrby(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::INCRBY)),
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
        return;
    };

    let Some(number) = terms_iter.next() else {
        return;
    };

    let Ok(number) = bytes_to_i64(&number) else {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectUsage(Command::INCRBY)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }
        return;
    };

    temple.incrby(
        key,
        number,
        tx,
        token,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );

    // if let Some(key) = terms_iter.next() {
    //     temple.incr(
    //         key,
    //         tx,
    //         token,
    //         SystemTime::now()
    //             .duration_since(UNIX_EPOCH)
    //             .map(|d| d.as_secs())
    //             .unwrap_or(0),
    //     );
    // } else if tx
    //     .send(Decree::Deliver(Gift {
    //         token,
    //         response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::INCR)),
    //     }))
    //     .is_err()
    // {
    //     eprintln!("Failed to send command response: channel closed");
    // }
}
