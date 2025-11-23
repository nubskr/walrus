use std::io::Cursor;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

//! Kafka facade: protocol parsing/encoding only. It delegates all stateful work to
//! `NodeController` (route append/read, metadata, membership). Keep it dumb: decode → call
//! controller → encode.

use crate::controller::NodeController;
use crate::kafka::codec::{decode_string_array, KafkaPrimitive, RequestHeader};
use crate::kafka::protocol::{encode_api_versions_response, encode_topics};
use crate::metadata::ClusterState;

const LOOPBACK_HOST: &str = "127.0.0.1";
type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
type HandlerFn =
    fn(&mut BytesMut, &mut Cursor<&[u8]>, &Arc<NodeController>, u16) -> HandlerFuture<'_>;

pub async fn run_server(port: u16, controller: Arc<NodeController>) -> Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    info!("Kafka facade listening on :{port}");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        info!("Accepted Kafka connection from {}", addr);
        let ctrl = controller.clone();
        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024 * 1024);
            loop {
                if buf.capacity() - buf.len() < 4096 {
                    buf.reserve(1024 * 1024);
                }
                match socket.read_buf(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        while buf.len() >= 4 {
                            let mut len_cursor = Cursor::new(&buf[..4]);
                            let frame_len = len_cursor.get_i32() as usize;
                            if buf.len() < 4 + frame_len {
                                let needed = 4 + frame_len - buf.len();
                                if buf.capacity() - buf.len() < needed {
                                    buf.reserve(needed);
                                }
                                break;
                            }
                            buf.advance(4);
                            let frame_bytes = buf.split_to(frame_len);
                            let frame = frame_bytes.freeze();
                            let mut cursor = Cursor::new(frame.as_ref());
                            let header = match RequestHeader::decode(&mut cursor) {
                                Ok(h) => h,
                                Err(e) => {
                                    error!("Failed to decode Kafka header: {e}");
                                    return;
                                }
                            };
                            match handle_request(header, &mut cursor, &ctrl, port).await {
                                Ok(response) => {
                                    if let Err(e) = write_frame(&mut socket, response).await {
                                        error!("socket write error: {e}");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    error!("request handling error: {e}");
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("socket read error: {e}");
                        break;
                    }
                }
            }
        });
    }
}

async fn write_frame(socket: &mut tokio::net::TcpStream, payload: Vec<u8>) -> Result<()> {
    let mut resp_buf = BytesMut::with_capacity(payload.len() + 4);
    resp_buf.put_i32(payload.len() as i32);
    resp_buf.extend_from_slice(&payload);
    socket.write_all(&resp_buf).await?;
    Ok(())
}

async fn handle_request(
    header: RequestHeader,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    advertised_port: u16,
) -> Result<Vec<u8>> {
    let mut res = BytesMut::new();
    header.encode(&mut res);

    let Some(handler) = handler_for(header.api_key) else {
        return Err(anyhow!("unsupported Kafka API key {}", header.api_key));
    };
    handler(&mut res, buf, controller, advertised_port).await?;

    Ok(res.to_vec())
}

fn handler_for(api_key: i16) -> Option<HandlerFn> {
    match api_key {
        0 => Some(handle_produce),
        1 => Some(handle_fetch),
        3 => Some(handle_metadata),
        18 => Some(handle_api_versions),
        19 => Some(handle_create_topics),
        50 => Some(handle_internal_state),
        51 => Some(handle_membership),
        60 => Some(handle_test_control),
        61 => Some(handle_force_monitor_error),
        62 => Some(handle_force_dir_error),
        63 => Some(handle_force_gc_error),
        _ => None,
    }
}

fn handle_metadata(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move {
        let topics = decode_string_array(buf)?;
        encode_metadata_response(
            res,
            controller.metadata.snapshot_state(),
            controller.node_id as i32,
            advertised_port,
            topics,
        );
        Ok(())
    })
}

fn handle_internal_state(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move {
        let topics = decode_string_array(buf)?;
        encode_internal_state_response(res, controller.metadata.snapshot_state(), topics);
        Ok(())
    })
}

fn handle_api_versions(
    res: &mut BytesMut,
    _buf: &mut Cursor<&[u8]>,
    _controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move {
        encode_api_versions_response(res);
        Ok(())
    })
}

fn handle_fetch(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move { encode_fetch_response(res, buf, controller).await })
}

fn handle_produce(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move { encode_produce_response(res, buf, controller).await })
}

fn handle_create_topics(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move { encode_create_topics_response(res, buf, controller).await })
}

fn handle_membership(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move { encode_membership_response(res, buf, controller).await })
}

fn handle_test_control(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move { encode_test_control_response(res, buf, controller).await })
}

fn handle_force_monitor_error(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move {
        let flag = i16::decode(buf)?;
        let resp = controller
            .handle_rpc(crate::rpc::InternalOp::TestControl(
                crate::rpc::TestControl::ForceMonitorError,
            ))
            .await;
        let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
        res.put_i16(code | (flag != 0) as i16);
        Ok(())
    })
}

fn handle_force_dir_error(
    res: &mut BytesMut,
    _buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move {
        let resp = controller
            .handle_rpc(crate::rpc::InternalOp::TestControl(
                crate::rpc::TestControl::ForceDirSizeError,
            ))
            .await;
        let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
        res.put_i16(code);
        Ok(())
    })
}

fn handle_force_gc_error(
    res: &mut BytesMut,
    _buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
    _advertised_port: u16,
) -> HandlerFuture<'_> {
    Box::pin(async move {
        let resp = controller
            .handle_rpc(crate::rpc::InternalOp::TestControl(
                crate::rpc::TestControl::ForceGcError,
            ))
            .await;
        let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
        res.put_i16(code);
        Ok(())
    })
}

fn encode_metadata_response(
    res: &mut BytesMut,
    state: ClusterState,
    local_node_id: i32,
    port: u16,
    topics_filter: Vec<String>,
) {
    res.put_i32(1);
    res.put_i32(local_node_id);
    String::from(LOOPBACK_HOST).encode(res);
    res.put_i32(port as i32);
    encode_topics(res, state, topics_filter, |res, name, mut parts| {
        res.put_i16(0);
        name.encode(res);
        parts.sort_by_key(|(id, _)| *id);
        res.put_i32(parts.len() as i32);
        for (partition_id, state) in parts {
            res.put_i16(0);
            res.put_i32(partition_id as i32);
            res.put_i32(state.leader_node as i32);
            res.put_i32(1);
            res.put_i32(state.leader_node as i32);
            res.put_i32(1);
            res.put_i32(state.leader_node as i32);
        }
    });
}

async fn encode_produce_response(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
) -> Result<()> {
    let _acks = i16::decode(buf)?;
    let _timeout = i32::decode(buf)?;
    let topic_count = i32::decode(buf)?;
    if topic_count < 0 {
        return Err(anyhow!("invalid topic count"));
    }

    let mut topic_results = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let topic = String::decode(buf)?;
        let partition_count = i32::decode(buf)?;
        if partition_count < 0 {
            return Err(anyhow!("invalid partition count"));
        }
        let mut partitions = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            let partition_id = i32::decode(buf)?;
            let record_set_size = i32::decode(buf)?;
            if record_set_size < 0 {
                return Err(anyhow!("invalid record set size"));
            }
            let size = record_set_size as usize;
            if buf.remaining() < size {
                return Err(anyhow!("record set truncated"));
            }
            let bytes = buf.copy_to_bytes(size).to_vec();
            let result = controller
                .route_and_append(&topic, partition_id as u32, bytes)
                .await
                .map(|_| 0i16)
                .unwrap_or_else(|e| {
                    error!("Produce failure for {}-{}: {}", topic, partition_id, e);
                    6
                });
            partitions.push((partition_id, result));
        }
        topic_results.push((topic, partitions));
    }

    res.put_i32(topic_results.len() as i32);
    for (topic, partitions) in topic_results {
        topic.encode(res);
        res.put_i32(partitions.len() as i32);
        for (partition_id, error_code) in partitions {
            res.put_i32(partition_id);
            res.put_i16(error_code);
            res.put_i64(0);
        }
    }
    Ok(())
}

async fn encode_fetch_response(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
) -> Result<()> {
    let _replica_id = i32::decode(buf)?; // unused
    let _max_wait = i32::decode(buf)?; // unused
    let _min_bytes = i32::decode(buf)?; // unused
    let topic_count = i32::decode(buf)?;
    if topic_count < 0 {
        return Err(anyhow!("invalid topic count"));
    }

    let mut topic_results = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let topic = String::decode(buf)?;
        let partition_count = i32::decode(buf)?;
        if partition_count < 0 {
            return Err(anyhow!("invalid partition count"));
        }
        let mut parts = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            let partition = i32::decode(buf)?;
            let fetch_offset = i64::decode(buf)?;
            let max_bytes = i32::decode(buf)?;
            let err_tuple = (partition, 1i16, 0i64, Vec::new());
            if fetch_offset < 0 {
                parts.push(err_tuple);
                continue;
            }
            let read_res = controller
                .route_and_read(
                    &topic,
                    partition as u32,
                    fetch_offset as u64,
                    max_bytes as usize,
                )
                .await;
            match read_res {
                Ok((data, observed_high_watermark)) => {
                    let high_watermark = observed_high_watermark as i64;
                    let payload = data.into_iter().next().unwrap_or_default();
                    let error_code = if payload.is_empty() {
                        if observed_high_watermark > 0
                            && fetch_offset as u64 >= observed_high_watermark
                        {
                            1i16
                        } else {
                            0i16
                        }
                    } else {
                        0i16
                    };
                    parts.push((partition, error_code, high_watermark, payload));
                }
                Err(e) => {
                    error!("Fetch failure for {}-{}: {}", topic, partition, e);
                    parts.push(err_tuple);
                }
            }
        }
        topic_results.push((topic, parts));
    }

    res.put_i32(topic_results.len() as i32);
    for (topic, partitions) in topic_results {
        topic.encode(res);
        res.put_i32(partitions.len() as i32);
        for (partition, error_code, high_watermark, payload) in partitions {
            res.put_i32(partition);
            res.put_i16(error_code);
            res.put_i64(high_watermark);
            res.put_i32(payload.len() as i32);
            res.extend_from_slice(&payload);
        }
    }
    Ok(())
}

fn encode_internal_state_response(
    res: &mut BytesMut,
    state: ClusterState,
    topics_filter: Vec<String>,
) {
    encode_topics(res, state, topics_filter, |res, name, mut parts| {
        name.encode(res);
        parts.sort_by_key(|(id, _)| *id);
        res.put_i32(parts.len() as i32);
        for (partition_id, state) in parts {
            res.put_i32(partition_id as i32);
            res.put_i32(state.leader_node as i32);
            res.put_i64(state.current_generation as i64);
        }
    });
}

async fn encode_create_topics_response(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
) -> Result<()> {
    let requested = i32::decode(buf)?;
    if requested < 0 {
        return Err(anyhow!("invalid topic count"));
    }
    let mut results = Vec::with_capacity(requested as usize);
    for _ in 0..requested {
        let name = String::decode(buf)?;
        let partitions = i32::decode(buf)?;
        let replication = i16::decode(buf)?;
        let assignment_count = i32::decode(buf)?;
        for _ in 0..assignment_count {
            let _ = i32::decode(buf)?; // partition id
            let replica_count = i32::decode(buf)?;
            for _ in 0..replica_count {
                let _ = i32::decode(buf)?;
            }
        }
        let config_count = i32::decode(buf)?;
        for _ in 0..config_count {
            let _ = String::decode(buf)?;
            let _ = String::decode(buf)?;
        }

        let err = if replication != 1 {
            40i16 // invalid replication factor
        } else if partitions <= 0 {
            21i16 // invalid partitions
        } else {
            match controller
                .create_topic(name.clone(), partitions as u32)
                .await
            {
                Ok(_) => 0i16,
                Err(e) => {
                    error!("CreateTopic failed for {}: {}", name, e);
                    6i16
                }
            }
        };
        results.push((name, err));
    }
    // Consume optional timeout (v0: required)
    let _timeout = i32::decode(buf).unwrap_or(0);

    res.put_i32(0); // throttle time
    res.put_i32(results.len() as i32);
    for (name, code) in results {
        name.encode(res);
        res.put_i16(code);
        "".to_string().encode(res); // error message
        res.put_i32(0); // configs count in response
    }
    Ok(())
}

async fn encode_membership_response(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
) -> Result<()> {
    // op_code: i16 (0 = remove voter), payload: node_id (i32)
    let op = i16::decode(buf)?;
    let target = i32::decode(buf)?;
    let mut code = 0i16;
    if op != 0 {
        code = 42; // unsupported op
    } else if let Err(e) = controller.remove_node_from_membership(target as u64).await {
        error!("membership change failed: {}", e);
        code = 6;
    }
    res.put_i16(code);
    Ok(())
}

async fn encode_test_control_response(
    res: &mut BytesMut,
    buf: &mut Cursor<&[u8]>,
    controller: &Arc<NodeController>,
) -> Result<()> {
    let op = i16::decode(buf)?;
    match op {
        0 => {
            let flag = i16::decode(buf)?;
            let resp = controller
                .handle_rpc(crate::rpc::InternalOp::TestControl(
                    crate::rpc::TestControl::ForceForwardReadError(flag != 0),
                ))
                .await;
            let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
            res.put_i16(code);
        }
        1 => {
            let topic = String::decode(buf)?;
            let partition = i32::decode(buf)?;
            let resp = controller
                .handle_rpc(crate::rpc::InternalOp::TestControl(
                    crate::rpc::TestControl::RevokeLeases {
                        topic,
                        partition: partition as u32,
                    },
                ))
                .await;
            let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
            res.put_i16(code);
        }
        2 => {
            let resp = controller
                .handle_rpc(crate::rpc::InternalOp::TestControl(
                    crate::rpc::TestControl::SyncLeases,
                ))
                .await;
            let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
            res.put_i16(code);
        }
        3 => {
            let node_id = i32::decode(buf)?;
            let addr = String::decode(buf)?;
            let resp = controller
                .handle_rpc(crate::rpc::InternalOp::TestControl(
                    crate::rpc::TestControl::TriggerJoin {
                        node_id: node_id as u64,
                        addr,
                    },
                ))
                .await;
            let code = matches!(resp, crate::rpc::InternalResp::Error(_)) as i16;
            res.put_i16(code);
        }
        other => {
            error!("unknown test control op {}", other);
            res.put_i16(42);
        }
    }
    Ok(())
}
