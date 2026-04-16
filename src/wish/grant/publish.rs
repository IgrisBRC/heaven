use std::sync::mpsc::Sender;

use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};

pub fn publish(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() != 3 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::PUBLISH)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        }

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(event), Some(message)) = (terms_iter.next(), terms_iter.next()) {
        temple.publish(tx, event, message, token);
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::PUBLISH)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    }
}
