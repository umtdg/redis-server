use std::{collections::HashMap, sync::Arc};

use bytes::BytesMut;
use futures::SinkExt;
use log::{debug, error, info};
use parking_lot::Mutex;
use resp::{
    parser::RespParser,
    types::{BulkString, RespReadable, RespValue, RespWritable},
    writer::{RespWriter, WriteBuf},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, Framed};

#[derive(Debug)]
enum Command {
    Get { key: BulkString },
    Set { key: BulkString, value: BulkString },
    Del { key: BulkString },
}

impl Command {
    fn get(cmd: &Vec<BulkString>) -> Option<Command> {
        if cmd.len() != 2 {
            None
        } else {
            Some(Command::Get {
                key: cmd[1].clone(),
            })
        }
    }

    fn set(cmd: &Vec<BulkString>) -> Option<Command> {
        if cmd.len() != 3 {
            None
        } else {
            Some(Command::Set {
                key: cmd[1].clone(),
                value: cmd[2].clone(),
            })
        }
    }

    fn del(cmd: &Vec<BulkString>) -> Option<Command> {
        if cmd.len() != 2 {
            None
        } else {
            Some(Command::Del {
                key: cmd[1].clone(),
            })
        }
    }

    fn from_cmd(cmd: &Vec<BulkString>) -> Option<Command> {
        let command = cmd.first();
        if let None = command {
            return None;
        }

        let command = command.unwrap().value();
        match &command[..] {
            "GET" => Self::get(cmd),
            "SET" => Self::set(cmd),
            "DEL" => Self::del(cmd),
            _ => None,
        }
    }

    fn handle(&self, db: &Arc<Database>, writer: &mut RespWriter<'_>) {
        info!("Handle: {:?}", *self);

        let mut db = db.kv_store.lock();

        let res = match self {
            &Command::Get { ref key } => match db.get(key.value()) {
                Some(v) => RespValue::Bulk(BulkString::new(v.clone())),
                None => RespValue::None,
            },
            &Command::Set { ref key, ref value } => {
                let key = key.value();
                let value = value.value();

                db.insert(key.clone(), value.clone());
                RespValue::Simple("OK".to_string())
            }
            &Command::Del { ref key } => match db.remove(key.value()) {
                Some(_) => RespValue::Simple("OK".to_string()),
                None => RespValue::None,
            },
        };

        res.write(writer).unwrap();
    }
}

type KvStore = HashMap<String, String>;

struct Database {
    kv_store: Mutex<KvStore>,
}

async fn send_err(
    transport: &mut Framed<TcpStream, BytesCodec>,
    msg: String,
    writer: &mut RespWriter<'_>,
) {
    error!("{}", msg);
    let res = RespValue::Error(msg);
    res.write(writer).unwrap();

    // TODO: This should be handled better
    let mut buf = BytesMut::with_capacity(writer.buffer().len());
    buf.extend_from_slice(writer.buffer().get().as_slice());
    if let Err(send_err) = transport.send(buf).await {
        error!("Failed to send error response: {:?}", send_err);
    }
}

async fn handle_request(
    transport: &mut Framed<TcpStream, BytesCodec>,
    req_buf: BytesMut,
    writer: &mut RespWriter<'_>,
    db: &Arc<Database>,
) {
    let mut parser = RespParser::new(&req_buf);
    let request = Vec::<BulkString>::parse(&mut parser);
    if let Err(err) = request {
        send_err(transport, format!("Error when parsing: {:?}", err), writer).await;
        return;
    }

    let cmd = request.unwrap();
    let command = Command::from_cmd(&cmd);
    if let None = command {
        send_err(
            transport,
            format!(
                "Unknown command {:?} with args {:?}",
                cmd.first(),
                &cmd[1..]
            ),
            writer,
        )
        .await;
        return;
    }

    command.unwrap().handle(db, writer);

    // TODO: This should be handled better
    let mut buf = BytesMut::with_capacity(writer.buffer().len());
    buf.extend_from_slice(writer.buffer().get().as_slice());
    if let Err(send_err) = transport.send(buf).await {
        error!("Failed to send response: {:?}", send_err);
    }
}

async fn handle_connection(stream: TcpStream, db: &Arc<Database>) {
    let peer_addr = stream.peer_addr().unwrap();
    debug!("Peer connected {:?}", peer_addr);
    let mut transport = Framed::new(stream, BytesCodec::new());

    while let Some(result) = transport.next().await {
        let mut write_buf = WriteBuf::new(Vec::new());
        let mut writer = RespWriter::new(&mut write_buf);

        if let Err(err) = result {
            send_err(
                &mut transport,
                format!("Error when receiving: {:?}", err),
                &mut writer,
            )
            .await;

            continue;
        }

        handle_request(&mut transport, result.unwrap(), &mut writer, db).await;
    }

    debug!("Peer disconnected {:?}", peer_addr);
}

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default()
        .filter_or("REDIS_LOG_LEVEL", "info")
        .write_style_or("REDIS_LOG_STYLE", "always");
    env_logger::init_from_env(env);

    info!("Initializing key-value store");
    let initial_db = KvStore::new();
    let db = Arc::new(Database {
        kv_store: Mutex::new(initial_db),
    });

    let listen_addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(listen_addr).await.unwrap();
    info!("Listening on {}", listen_addr);

    loop {
        match listener.accept().await {
            Err(err) => error!("Error when establishing connection: {:?}", err),
            Ok((stream, _)) => {
                let db = db.clone();
                tokio::spawn(async move {
                    handle_connection(stream, &db).await;
                });
            }
        }
    }
}
