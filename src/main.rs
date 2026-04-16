#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use clap::Parser;
use std::path::PathBuf;

mod server;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long = "bind", default_value = "127.0.0.1")]
    ip: String,

    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long = "io-threads", default_value_t = 3)]
    io_threads: usize,

    #[arg(long = "event-limit", default_value_t = 128)]
    event_limit: usize,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,
}

fn main() {
    let args = Args::parse();

    let dir = args
        .dir
        .unwrap_or_else(|| std::env::current_dir().expect("Couldn't access current directory"));

 if let Err(e) = server::run(
 &args.ip,
 args.port,
 args.io_threads,
 args.event_limit,
 dir,
 &args.dbfilename,
 0,
 "no",
 ) {
 eprintln!("Server failed to start: {}", e);
 std::process::exit(1);
 }
}
