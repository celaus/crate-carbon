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

struct CarbonMessage {
    pub prefix: String,
    pub timestamp: i64,
    pub value: i64,
}

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
        .get_matches();

    log4rs::init_file("logging.yml", Default::default()).unwrap();

    let cluster_addr = matches.value_of("cratedb-address").unwrap_or("http://127.0.0.1:4200").to_string();

    let (tx, rx) = channel::<CarbonMessage>();

    let inserter = thread::spawn(move || {
        let mut cluster = cratedb::Cluster::from_string(cluster_addr).unwrap();
        let data_channel = rx;
        let batch_size = 1000;
        let mut current_batch = 0;
        if let Err(e) = cluster.query("create table if not exists carbon.metrics(agent string, ts timestamp, value long, day as date_trunc('day', ts)) partitioned by (day)", None) {
            panic!("{:?}", e);
        }
        let max_timeout = Duration::from_secs(90);

        let insert_stmt: String = "insert into carbon.metrics(agent, ts, value) values(?,?,?)".to_string();
        info!("Using statement '{}' to insert data.", insert_stmt);
        loop {
            let mut v: Vec<(String, i64, i64)> = Vec::with_capacity(batch_size);

            loop {
                if let Ok(msg) = data_channel.recv_timeout(max_timeout) {
                    v.push((msg.prefix, msg.timestamp, msg.value));

                    current_batch += 1;
                    if current_batch == batch_size {
                        break;
                    }
                }
                else {
                    info!("No data received for {:?}. Current queue size: {}", max_timeout, current_batch);
                    break;
                }
            }
            if current_batch > 0 {
                if let Err(e) = cluster.bulk_query(&insert_stmt, Box::new(v)) {
                    panic!("Error inserting data: {:?}", e);
                } else {
                    debug!("Bulk insert done, {} items inserted", current_batch);
                    current_batch = 0;
                }
            }
        }
    });
    let addr = "127.0.0.1:2003";
    mioco::start(move || {
        let parsed_addr = addr.parse::<SocketAddr>().unwrap();
            let listener = TcpListener::bind(&parsed_addr).unwrap();
            info!("Starting carbon relay server on {:?}",
                  listener.local_addr().unwrap());
            loop {

                let conn = listener.accept().unwrap();
                let s = tx.clone();
                mioco::spawn(move || -> io::Result<()> {
                    let mut reader = BufReader::with_capacity(200, conn);
                    let mut buf = String::new();
                    while reader.read_line(&mut buf).unwrap() > 0 {
                        let v: Vec<&str> = buf.split_whitespace().take(3).collect();
                        let _ = s.send(CarbonMessage {
                            timestamp: v[2].parse::<i64>().unwrap(),
                            value: v[1].parse::<i64>().unwrap(),
                            prefix: v[0].to_string(),
                        });

                    }
                    Ok(())
                });
            }
        })
        .unwrap();
    let _ = inserter.join();
}
