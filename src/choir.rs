use crossbeam_channel::{Receiver, Sender, unbounded};

type Song = Box<dyn FnOnce() + Send + 'static>;

struct Angel {
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Angel {
    fn new(rx: Receiver<Song>) -> Self {
        Angel {
            thread: Some(std::thread::spawn(move || {
                while let Ok(song) = rx.recv() {
                    song();
                }
            })),
        }
    }
}

pub struct Choir {
    angels: Vec<Angel>,
    tx: Option<Sender<Song>>,
}

impl Choir {
    pub fn new(capacity: usize) -> Self {
        let mut angels = Vec::with_capacity(capacity);
        let (tx, rx) = unbounded();

        for _ in 0..capacity {
            angels.push(Angel::new(rx.clone()));
        }

        Choir {
            angels,
            tx: Some(tx),
        }
    }

    pub fn sing<F>(&self, song: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(tx) = &self.tx {
            tx.send(Box::new(song)).unwrap();
        }
    }
}

impl Drop for Choir {
    fn drop(&mut self) {
        drop(self.tx.take());

        for angel in &mut self.angels {
            angel.thread.take().unwrap().join().unwrap();
        }
    }
}
