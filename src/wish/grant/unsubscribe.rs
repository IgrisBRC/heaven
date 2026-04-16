use std::sync::mpsc::Sender;

use mio::Token;

use crate::{temple::Temple, wish::grant::Decree};

pub fn unsubscribe(terms: Vec<Vec<u8>>, temple: &mut Temple, tx: Sender<Decree>, token: Token) {
    let mut terms_iter = terms.into_iter();
    terms_iter.next();

    temple.unsubscribe(tx, token, terms_iter.collect());
}
