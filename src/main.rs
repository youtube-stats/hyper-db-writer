extern crate chrono;
extern crate futures;
extern crate hyper;
extern crate postgres;
extern crate quick_protobuf;

pub mod types;
use types::SubStoreDatum;

pub mod statics;
use statics::{CACHE_SIZE, POSTGRESQL_URL, INSERT};

pub mod message;
use message::{Ack, SubMessage};

use crate::hyper::{Response, Server, Body, Request};
use crate::hyper::rt::{Future, Stream, run};
use crate::postgres::{Connection, TlsMode};
use crate::postgres::transaction::Transaction;
use crate::quick_protobuf::{serialize_into_vec, deserialize_from_slice};
use ::std::net::SocketAddr;
use ::std::sync::mpsc::{Sender, Receiver};
use ::std::sync::mpsc::channel;
use ::std::thread;
use crate::statics::DRAIN_RANGE;
use std::ops::Range;
use hyper::service::service_fn;
use chrono::{DateTime, Local};

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
                            let time: DateTime<Local> = Local::now();

                            for i in 0..other.ids.len() {
                                let id: i32 = other.ids[i];
                                let sub: i32 = other.subs[i];

                                let value: SubStoreDatum = SubStoreDatum {
                                    time,
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

                                let time: &DateTime<Local> = &sub_row.time;
                                let id: &i32 =  &sub_row.id;
                                let sub: &i32 = &sub_row.sub;

                                trans.execute(query, &[time, id, sub])
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

            service_fn(move |req: Request<Body>| {
                let sx: Sender<SubMessage> = {
                    let sx: &Sender<SubMessage> = &sx;
                    sx.clone()
                };

                req.into_body()
                    .concat2()
                    .and_then(move |entire_body| {
                        let sx: Sender<SubMessage> = {
                            let sx: &Sender<SubMessage> = &sx;
                            sx.clone()
                        };

                        let good_resp = {
                            let mut message: Ack = Ack::default();
                            message.ok = true;
                            let vec: Vec<u8> = serialize_into_vec(&message)
                                .expect("Cannot serialize `foobar`");

                            let body: Body = Body::from(vec);
                            Response::new(body)
                        };

                        let bytes: Vec<u8> = entire_body.to_vec();
                        let bytes: &[u8] = bytes.as_slice();
                        let t: SubMessage = deserialize_from_slice(bytes)
                            .expect("Could not deserialize");

                        sx.send(t).clone()
                            .expect("Could not send message");

                        Ok(good_resp)
                    })
            })
        };

        let server = Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| eprintln!("server error: {}", e));

        run(server);
    }
}
