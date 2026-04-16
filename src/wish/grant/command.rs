use crate::wish::{Command, Sacrilege};
use std::sync::mpsc::Sender;

use mio::Token;

use crate::wish::{
    InfoType, Response,
    grant::{Decree, Gift},
};

pub fn command(terms: Vec<Vec<u8>>, tx: Sender<Decree>, token: Token) {
    if terms.len() != 1
        && tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::COMMAND)),
            }))
            .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
        return;
    }

    if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Info(InfoType::Command),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed");
    };
}
