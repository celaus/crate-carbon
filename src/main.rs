#[macro_use]
extern crate mioco;
extern crate clap;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate cratedb;

use std::thread;

use std::io;
use std::io::BufReader;
use std::io::BufRead;
use mioco::tcp::TcpListener;
use std::time::Duration;
use clap::{Arg, App};
use std::net::SocketAddr;

use std::sync::mpsc::channel;

fn main() {
    let matches = App::new("Carbon CrateDB Relay Server")
        .version("0.1.0")
        .author("Claus Matzinger. <claus.matzinger+kb@gmail.com>")
        .about("Receives Carbon Plaintext messages and inserts them into CrateDB")
        .arg(Arg::with_name("cratedb-address")
            .short("c")
            .long("cratedb")
            .help("CrateDB Cluster address, can have multiple hosts.")
            .value_name("127.0.0.1:4200")
            .takes_value(true))
        .arg(Arg::with_name("listen-address")
            .short("l")
            .long("listen")
            .help("Local address to listen to.")
            .value_name("127.0.0.1:2003")
            .takes_value(true))
        .arg(Arg::with_name("batch-size")
            .short("b")
            .long("batch-size")
            .help("Batch size for CrateDB inserts.")
            .value_name("1000")
            .takes_value(true))

        .get_matches();

    log4rs::init_file("logging.yml", Default::default()).unwrap();

    let cluster_addr =
        matches.value_of("cratedb-address").unwrap_or("http://127.0.0.1:4200").to_string();

    let (tx, rx) = channel::<Vec<(String, i64, i64)>>();

    let inserter = thread::spawn(move || {
        let mut cluster = cratedb::Cluster::from_string(cluster_addr).unwrap();
        let data_channel = rx;
        if let Err(e) = cluster.query("create table if not exists carbon.metrics(agent string, \
            ts timestamp, value long, day as date_trunc('day', ts)) partitioned by (day)", None) {
            panic!("{:?}", e);
        }
        let max_timeout = Duration::from_secs(90);

        let insert_stmt =  "insert into carbon.metrics(agent, value, ts) values(?,?,?)";

        info!("Using statement '{}' to insert data.", insert_stmt);
        loop {
            loop {
                if let Ok(msg) = data_channel.recv_timeout(max_timeout) {
                    if let Err(e) = cluster.bulk_query(&insert_stmt, Box::new(msg)) {
                        panic!("Error inserting data: {:?}", e);
                    }

                } else {
                    info!("No data received for {:?}",
                          max_timeout);
                    break;
                }
            }
        }
    });

    let addr = matches.value_of("cratedb-address").unwrap_or("127.0.0.1:2003").to_string();
    let batch_size = matches.value_of("cratedb-address").unwrap_or("1000").parse::<usize>().unwrap_or(1000);

    mioco::start(move || {
            let parsed_addr = addr.parse::<SocketAddr>().unwrap();
            let listener = TcpListener::bind(&parsed_addr).unwrap();
            info!("Starting carbon relay server on {:?}",
                  listener.local_addr().unwrap());
            loop {

                let conn = listener.accept().unwrap();
                let q = tx.clone();
                mioco::spawn(move || -> io::Result<()> {

                    let mut reader = BufReader::new(conn);
                    loop {
                        let mut send_buffer: Vec<(String, i64, i64)> =
                            Vec::with_capacity(batch_size);
                        let mut buf = String::new();
                        loop {
                            if reader.read_line(&mut buf).unwrap() > 0 {
                                let v: Vec<&str> = buf.split_whitespace().take(3).collect();
                                send_buffer.push((v[0].to_string(),
                                                  v[1].parse::<i64>().unwrap(),
                                                  v[2].parse::<i64>().unwrap()));
                                if send_buffer.len() == batch_size {
                                    break;
                                }
                            } else {
                                break;
                            }

                        }
                        if send_buffer.len() > 0 {
                            let _ = q.send(send_buffer);
                        } else {
                            break;
                        }
                    }
                    Ok(())
                });
            }
        })
        .unwrap();
    let _ = inserter.join();
}
