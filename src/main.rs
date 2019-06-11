extern crate chrono;
extern crate hyper;
extern crate postgres;
extern crate quick_protobuf;

pub mod types;
use types::SubStoreDatum;

pub mod statics;
use statics::{CACHE_SIZE, POSTGRESQL_URL, INSERT};

pub mod message;
use message::{Ack, SubMessage};

use crate::hyper::{Response, Server, Body, Request, Method};
use crate::hyper::rt::{Future, Stream, run};
use crate::hyper::service::service_fn_ok;
use crate::postgres::{Connection, TlsMode};
use crate::postgres::transaction::Transaction;
use crate::quick_protobuf::{serialize_into_vec, deserialize_from_slice};
use ::std::net::SocketAddr;
use ::std::sync::mpsc::{Sender, Receiver};
use ::std::sync::mpsc::channel;
use ::std::thread;
use crate::statics::DRAIN_RANGE;
use std::ops::Range;

pub fn main() {
    let (sx, rx): (Sender<SubMessage>, Receiver<SubMessage>) = channel();

    {
        let f = move || {
            let conn: Connection = {
                let params: &'static str = POSTGRESQL_URL;
                let tls: TlsMode = TlsMode::None;

                Connection::connect(params, tls)
                    .expect("Could not connect to database")
            };
            let mut store: Vec<SubStoreDatum> = {
                let capacity: usize = 2 * CACHE_SIZE;
                Vec::with_capacity(capacity)
            };

            loop {
                {
                    println!("Waiting for message");
                    let other: SubMessage = rx.recv()
                        .expect("Could not retrieve message");
                    println!("Got message {:?}", other);

                    {
                        let mut other: Vec<SubStoreDatum> = {
                            let mut msg_store: Vec<SubStoreDatum> = Vec::new();

                            for i in 0..other.ids.len() {
                                let id: i32 = other.ids[i];
                                let sub: i32 = other.subs[i];

                                let value: SubStoreDatum = SubStoreDatum {
                                    id,
                                    sub
                                };
                                msg_store.push(value);
                            }

                            msg_store
                        };
                        store.append(&mut other);
                    }
                    println!("New size of store is {}", store.len());

                    if store.len() >= CACHE_SIZE {
                        println!("Writing {} entries", CACHE_SIZE);

                        {
                            let trans: Transaction = conn.transaction()
                                .expect("Could not start transaction");

                            let query: &'static str = INSERT;
                            for i in DRAIN_RANGE {
                                let sub_row: &SubStoreDatum = &store[i];
                                let id: &i32 =  &sub_row.id;
                                let sub: &i32 = &sub_row.sub;

                                trans.execute(query, &[id, sub])
                                    .expect("Could not insert row");
                            }

                            trans.commit()
                                .expect("Could not commit transactrion block");
                        }

                        let range: Range<usize> = DRAIN_RANGE;
                        store.drain(range);

                        println!("New store size is {}", store.len());
                    }
                }
            }
        };
        thread::spawn(f);
    }

    {
        let addr: SocketAddr = ([0u8, 0u8, 0u8, 0u8], 8081u16).into();

        let new_service = move || {
            let sx: Sender<SubMessage> = {
                let sx: &Sender<SubMessage> = &sx;
                sx.clone()
            };

            service_fn_ok(move |req: Request<Body>| {
                println!("{:?}", req.body());

                let method: Method = req.method().clone();
                let path: String = {
                    let path: &str = req.uri().path();

                    path.to_string().clone()
                };

                let bytes: Vec<u8> = {
                    let body: Body = req.into_body();
                    let entire_body = body.concat2();
                    let mut bytes: Vec<u8> = Vec::new();

                    let _ = entire_body.map(|body| {
                        let other = body.into_bytes();
                        let mut other: Vec<u8> = other.to_vec();

                        println!("Read {} bytes", other.len());
                        bytes.append(&mut other);
                    });

                    bytes
                };
                let bytes: &[u8] = bytes.as_slice();
                let msg_option = deserialize_from_slice(bytes);

                let good_resp = {
                    let mut message: Ack = Ack::default();
                    message.ok = true;
                    let vec: Vec<u8> = serialize_into_vec(&message)
                        .expect("Cannot serialize `foobar`");

                    let body: Body = Body::from(vec);
                    Response::new(body)
                };
                let bad_resp = {
                    let mut message: Ack = Ack::default();
                    message.ok = false;
                    let vec: Vec<u8> = serialize_into_vec(&message)
                        .expect("Cannot serialize `foobar`");

                    let body: Body = Body::from(vec);
                    Response::new(body)
                };

                match (method, path, msg_option) {
                    (Method::POST, _endpoint, Ok(t)) => {
                        sx.send(t)
                            .expect("Could not send msg");
                        good_resp
                    },
                    _ => bad_resp
                }
            })
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        run(server);
    }
}
