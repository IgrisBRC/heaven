use mio::Token;

use crate::{
    temple::Temple,
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
    },
};
use std::sync::mpsc::Sender;

pub fn subscribe(
    terms: Vec<Vec<u8>>,
    temple: &mut Temple,
    tx: Sender<Decree>,
    token: Token,
)  {
    if terms.len() < 2 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SUBSCRIBE)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed");
        };

        return ;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.subscribe(tx, terms_iter.collect(), token);

    
}
