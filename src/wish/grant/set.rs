use mio::Token;

use crate::{
    temple::{Temple, soul::Value},
    wish::{
        Command, Response, Sacrilege,
        grant::{Decree, Gift},
        util::bytes_to_u64,
    },
};

use std::{
    sync::mpsc::Sender,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn set(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    if terms.len() > 5 {
        if tx
            .send(Decree::Deliver(Gift {
                token,
                response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SET)),
            }))
            .is_err()
        {
            eprintln!("Failed to send command response: channel closed")
        };

        return;
    }

    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    if let (Some(key), Some(value)) = (terms_iter.next(), terms_iter.next()) {
        match terms_iter.next() {
            Some(command) => {
                if command.eq_ignore_ascii_case(b"EX") {
                    let Some(expiry) = terms_iter.next() else {
                        if tx
                            .send(Decree::Deliver(Gift {
                                token,
                                response: Response::Error(Sacrilege::IncorrectUsage(Command::SET)),
                            }))
                            .is_err()
                        {
                            eprintln!("Failed to send command response: channel closed")
                        };

                        return;
                    };

                    let Ok(expiry) = bytes_to_u64(&expiry) else {
                        if tx
                            .send(Decree::Deliver(Gift {
                                token,
                                response: Response::Error(Sacrilege::IncorrectUsage(Command::SET)),
                            }))
                            .is_err()
                        {
                            eprintln!("Failed to send command response: channel closed")
                        };

                        return;
                    };

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);

                    temple.set(key, (Value::String(value), Some(now + expiry)), tx, token);
                } else if tx
                    .send(Decree::Deliver(Gift {
                        token,
                        response: Response::Error(Sacrilege::IncorrectNumberOfArguments(
                            Command::SET,
                        )),
                    }))
                    .is_err()
                {
                    eprintln!("Failed to send command response: channel closed")
                }
            }
            None => {
                temple.set(key, (Value::String(value), None), tx, token);
            }
        }
    } else if tx
        .send(Decree::Deliver(Gift {
            token,
            response: Response::Error(Sacrilege::IncorrectNumberOfArguments(Command::SET)),
        }))
        .is_err()
    {
        eprintln!("Failed to send command response: channel closed")
    }
}
