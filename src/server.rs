use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;

use heaven::choir::Choir;
use heaven::egress;
use heaven::temple::Temple;
use heaven::temple::soul::ServerError;
use heaven::wish::grant::Decree;
use heaven::wish::{self, Pilgrim};

use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};

/// Errors that can occur during server startup
#[derive(Debug)]
pub enum ServerStartupError {
    InvalidIpAddress(String),
    PollCreationFailed(std::io::Error),
    BindFailed(std::io::Error),
    InvalidPath(PathBuf),
}

impl std::fmt::Display for ServerStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIpAddress(ip) => write!(f, "Invalid IPv4 address: {}", ip),
            Self::PollCreationFailed(e) => write!(f, "Failed to create poll instance: {}", e),
            Self::BindFailed(e) => write!(f, "Failed to bind to address: {}", e),
            Self::InvalidPath(path) => write!(f, "Path contains invalid UTF-8: {:?}", path),
        }
    }
}

impl std::error::Error for ServerStartupError {}

pub fn run(
    ipv4_address: &str,
    port: u16,
    io_threads: usize,
    event_capacity: usize,
    dir: PathBuf,
    dbfilename: &str,
    max_memory: u64,
    append_only: &str,
) -> Result<(), ServerStartupError> {
    let ipv4_addr = Ipv4Addr::from_str(ipv4_address)
        .map_err(|_| ServerStartupError::InvalidIpAddress(ipv4_address.to_string()))?;
    let socket_addr_v4 = SocketAddrV4::new(ipv4_addr, port);
    let socket_addr = SocketAddr::V4(socket_addr_v4);

    let mut poll = Poll::new().map_err(ServerStartupError::PollCreationFailed)?;

    let mut listener = TcpListener::bind(socket_addr).map_err(ServerStartupError::BindFailed)?;

    const SERVER: Token = Token(0);

    let mut events = Events::with_capacity(event_capacity);

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)
        .map_err(|e| {
            eprintln!("Failed to register listener with poll: {}", e);
            ServerStartupError::PollCreationFailed(e)
        })?;

    let mut ingress_map: HashMap<Token, Pilgrim> = HashMap::new();

    let mut pilgrim_counter = 1;

    let ingress_choir = Choir::new(io_threads);

    let mut itoa_buf = itoa::Buffer::new();

    let dir_str = dir
        .to_str()
        .ok_or_else(|| ServerStartupError::InvalidPath(dir.clone()))?;

    let temple = Temple::worship(
        dir_str.into(),
        dbfilename.into(),
        ipv4_address.into(),
        itoa_buf.format(port).into(),
        itoa_buf.format(io_threads).into(),
        itoa_buf.format(event_capacity).into(),
        itoa_buf.format(max_memory).into(),
        append_only.into(),
    );

    let mut server_temple = temple.sanctify();

    let (ingress_tx, ingress_rx) = std::sync::mpsc::channel::<(Token, Pilgrim)>();
    let (egress_tx, egress_rx) = std::sync::mpsc::channel();
    let (pilgrim_tx, pilgrim_rx) = std::sync::mpsc::channel::<Decree>();

    let shutdown_tx = egress_tx.clone();

    std::thread::spawn(move || {
        egress::egress(pilgrim_rx, egress_tx);
    });

    if ctrlc::set_handler(move || {
        let (server_shutdown_tx, server_shutdown_rx) =
            std::sync::mpsc::channel::<Result<(), ServerError>>();

        server_temple.save(server_shutdown_tx, SERVER);

        if let Ok(Ok(())) = server_shutdown_rx.recv() {
            println!("Database snapshot saved successfuly");
        } else {
            println!("Couldn't save database snapshot");
        }

        if shutdown_tx.send(SERVER).is_err() {
            eprintln!("Ctrlc handler failed");
        }
    })
    .is_err()
    {
        eprintln!("Failed to set ctrlc handler");
    };

    loop {
        if poll
            .poll(&mut events, Some(std::time::Duration::from_millis(100)))
            .is_err()
        {
            eprintln!("poll() failed");
        }

        while let Ok((token, mut pilgrim)) = ingress_rx.try_recv() {
            if poll
                .registry()
                .reregister(&mut pilgrim.stream, token, Interest::READABLE)
                .is_err()
            {
                // eprintln!("reregister() failed");
            }

            ingress_map.insert(token, pilgrim);
        }

        while let Ok(token) = egress_rx.try_recv() {
            if token == SERVER {
                std::process::exit(0);
            }

            if let Some(mut pilgrim) = ingress_map.remove(&token)
                && poll.registry().deregister(&mut pilgrim.stream).is_err()
            {
                eprintln!("deregister() failed")
            }
        }

        for event in &events {
            let token = event.token();
            match token {
                SERVER => loop {
                    match listener.accept() {
                        Ok((mut stream, _address)) => {
                            let pilgrim_token = Token(pilgrim_counter);

                            if poll
                                .registry()
                                .register(&mut stream, pilgrim_token, Interest::READABLE)
                                .is_err()
                            {
                                eprintln!("register() failed");
                            }

                            let std_stream: TcpStream = stream.into();
                            let std_stream_clone = match std_stream.try_clone() {
                                Ok(clone) => clone,
                                Err(e) => {
                                    eprintln!(
                                        "Failed to clone socket for client {:?}: {}",
                                        pilgrim_token, e
                                    );
                                    continue;
                                }
                            };

                            let ingress_mio = mio::net::TcpStream::from_std(std_stream);
                            let egress_mio = mio::net::TcpStream::from_std(std_stream_clone);

                            pilgrim_counter += 1;

                            ingress_map.insert(
                                pilgrim_token,
                                Pilgrim {
                                    stream: ingress_mio,
                                    virtue: None,
                                    tx: pilgrim_tx.clone(),
                                },
                            );

                            if pilgrim_tx
                                .send(Decree::Welcome(pilgrim_token, egress_mio))
                                .is_err()
                            {
                                eprintln!(
                                    "Failed to send welcome to egress for client {:?}: channel closed",
                                    pilgrim_token
                                );
                                ingress_map.remove(&pilgrim_token);
                            }
                        }
                        Err(err) => {
                            if err.kind() == ErrorKind::WouldBlock {
                                break;
                            }
                        }
                    }
                },

                Token(token_number) => {
                    if let Some(mut pilgrim) = ingress_map.remove(&Token(token_number)) {
                        let sanctum = temple.sanctify();
                        let tx = ingress_tx.clone();
                        let pilgrim_tx = pilgrim_tx.clone();

                        ingress_choir.sing(move || {
                            match wish::wish(&mut pilgrim, sanctum, Token(token_number)) {
                                Ok(_) => {
                                    if tx.send((mio::Token(token_number), pilgrim)).is_err() {
                                 eprintln!("Failed to return client {:?} to event loop: channel closed", Token(token_number));
                                    }
                                }
                                Err(_e) => {
                                    if pilgrim_tx.send(Decree::Goodbye(Token(token_number))).is_err() {
                                 eprintln!("Failed to remove disconnected FD from FD map: channel closed");
                                    }
                                    // eprintln!("{:?}", e);
                                }
                            }
                        });
                    }
                }
            }
        }
    }
}
