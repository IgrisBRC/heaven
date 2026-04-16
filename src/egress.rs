use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::io::Write;
use crate::wish::grant::Decree;

use mio::Token;

mod send;

pub fn egress(pilgrim_rx: Receiver<Decree>, egress_tx: Sender<Token>) {
    let mut egress_map: HashMap<Token, mio::net::TcpStream> = HashMap::new();
    let mut buffer = Vec::with_capacity(2100);
    let mut itoa_buf = itoa::Buffer::new();

    loop {
        match pilgrim_rx.recv() {
            Ok(Decree::Welcome(token, stream)) => {
                egress_map.insert(token, stream);
            }
            Ok(Decree::Deliver(gift)) => {
                if let Some(stream) = egress_map.get_mut(&gift.token) {
                    let token = gift.token;

                    if send::send(stream, gift, &mut buffer).is_err()
                        && egress_tx.send(token).is_err()
                    {
                        eprintln!("Failed to send command response: channel closed");
                    };
                }
            }
            Ok(Decree::Broadcast(token, event, message, clients)) => {
                let clients_len = clients.len();

                let mut response = b"*3\r\n$7\r\nmessage\r\n$".to_vec();
                response.extend_from_slice(itoa_buf.format(event.len()).as_bytes());
                response.extend_from_slice(b"\r\n");
                response.extend_from_slice(&event);
                response.extend_from_slice(b"\r\n$");
                response.extend_from_slice(itoa_buf.format(message.len()).as_bytes());
                response.extend_from_slice(b"\r\n");
                response.extend_from_slice(&message);
                response.extend_from_slice(b"\r\n");

                for client in clients {
                    if let Some(stream) = egress_map.get_mut(&client)
                        && stream.write_all(&response).is_err()
                    {
                        eprintln!("writing to stream failed for client");
                    }
                }

                if let Some(publisher_stream) = egress_map.get_mut(&token) {
                    let mut response = b":".to_vec();
                    response.extend_from_slice(itoa_buf.format(clients_len).as_bytes());
                    response.extend_from_slice(b"\r\n");

                    if publisher_stream.write_all(&response).is_err() {
                        eprintln!("writing to stream failed for publisher");
                    }
                }
            }
            Err(_) => break,
        }
    }
}
