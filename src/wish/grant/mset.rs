use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::sync::mpsc::Sender;

pub fn mset(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    let terms_len = terms.len();

    if terms_len < 3 || terms_len.is_multiple_of(2) {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::MSET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };
        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.mset(terms_iter, tx, token);
}
