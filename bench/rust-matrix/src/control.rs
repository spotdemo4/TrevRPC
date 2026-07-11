use std::fs;
use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::BoxError;
use crate::events::{ControlRequest, ControlResponse};
use crate::metrics::ProcessSnapshot;

pub fn bind(path: &Path) -> Result<UnixListener, BoxError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(UnixListener::bind(path)?)
}

pub async fn serve_one(
    listener: UnixListener,
    run_id: &str,
    config_hash: &str,
) -> Result<(), BoxError> {
    let (stream, _) = listener.accept().await?;
    let mut stream = BufReader::new(stream);

    let begin = read_request(&mut stream).await?;
    match begin {
        ControlRequest::Begin {
            run_id: actual_run_id,
            config_hash: actual_hash,
        } if actual_run_id == run_id && actual_hash == config_hash => {}
        _ => return Err("invalid begin control request".into()),
    }
    write_response(
        stream.get_mut(),
        &ControlResponse::BeginAck {
            run_id: run_id.to_owned(),
        },
    )
    .await?;
    let start = ProcessSnapshot::capture()?;

    let end = read_request(&mut stream).await?;
    let (completed, failed) = match end {
        ControlRequest::End {
            run_id: actual_run_id,
            completed,
            failed,
        } if actual_run_id == run_id => (completed, failed),
        _ => return Err("invalid end control request".into()),
    };
    let server = start.delta(ProcessSnapshot::capture()?);
    write_response(
        stream.get_mut(),
        &ControlResponse::EndAck {
            run_id: run_id.to_owned(),
            completed,
            failed,
            server,
        },
    )
    .await
}

pub struct ControlClient {
    stream: BufReader<UnixStream>,
    run_id: String,
}

impl ControlClient {
    pub async fn connect(path: &Path, run_id: &str) -> Result<Self, BoxError> {
        Ok(Self {
            stream: BufReader::new(UnixStream::connect(path).await?),
            run_id: run_id.to_owned(),
        })
    }

    pub async fn begin(&mut self, config_hash: &str) -> Result<(), BoxError> {
        write_request(
            self.stream.get_mut(),
            &ControlRequest::Begin {
                run_id: self.run_id.clone(),
                config_hash: config_hash.to_owned(),
            },
        )
        .await?;
        match read_response(&mut self.stream).await? {
            ControlResponse::BeginAck { run_id } if run_id == self.run_id => Ok(()),
            _ => Err("invalid begin acknowledgement".into()),
        }
    }

    pub async fn end(
        &mut self,
        completed: u64,
        failed: u64,
    ) -> Result<crate::metrics::ProcessDelta, BoxError> {
        write_request(
            self.stream.get_mut(),
            &ControlRequest::End {
                run_id: self.run_id.clone(),
                completed,
                failed,
            },
        )
        .await?;
        match read_response(&mut self.stream).await? {
            ControlResponse::EndAck {
                run_id,
                completed: acknowledged_completed,
                failed: acknowledged_failed,
                server,
            } if run_id == self.run_id
                && acknowledged_completed == completed
                && acknowledged_failed == failed =>
            {
                Ok(server)
            }
            _ => Err("invalid end acknowledgement".into()),
        }
    }
}

async fn read_request(reader: &mut BufReader<UnixStream>) -> Result<ControlRequest, BoxError> {
    read_json_line(reader).await
}

async fn read_response(reader: &mut BufReader<UnixStream>) -> Result<ControlResponse, BoxError> {
    read_json_line(reader).await
}

async fn read_json_line<T>(reader: &mut BufReader<UnixStream>) -> Result<T, BoxError>
where
    T: serde::de::DeserializeOwned,
{
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Err("control connection closed".into());
    }
    Ok(serde_json::from_str(&line)?)
}

async fn write_request(stream: &mut UnixStream, value: &ControlRequest) -> Result<(), BoxError> {
    write_json_line(stream, value).await
}

async fn write_response(stream: &mut UnixStream, value: &ControlResponse) -> Result<(), BoxError> {
    write_json_line(stream, value).await
}

async fn write_json_line<T>(stream: &mut UnixStream, value: &T) -> Result<(), BoxError>
where
    T: serde::Serialize,
{
    let mut encoded = serde_json::to_vec(value)?;
    encoded.push(b'\n');
    stream.write_all(&encoded).await?;
    stream.flush().await?;
    Ok(())
}
