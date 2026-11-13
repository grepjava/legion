//! `legion volume backup` and `restore`: a Volume's data to a file and
//! back (DESIGN.md §4.3). The file is one line of the backup's header (its
//! JSON), then the snapshot's `zfs send` stream.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use legion_api::volume::VolumeBackupHeader;
use legion_proto::v1::volume_restore_chunk::Part;
use legion_proto::v1::volumes_client::VolumesClient;
use legion_proto::v1::{VolumeBackupRequest, VolumeRestoreChunk, VolumeRestoreStart};
use tonic::transport::Channel;

/// Bytes per message sent.
const CHUNK: usize = 1 << 20;

fn client(channel: Channel) -> VolumesClient<Channel> {
    VolumesClient::new(channel)
        .max_decoding_message_size(legion_manager::MAX_MESSAGE)
        .max_encoding_message_size(legion_manager::MAX_MESSAGE)
}

/// Writes the header line a backup file starts with.
fn write_header(out: &mut dyn Write, header: &[u8]) -> anyhow::Result<VolumeBackupHeader> {
    let (parsed, line) = VolumeBackupHeader::line(header).map_err(anyhow::Error::msg)?;
    out.write_all(&line)?;
    Ok(parsed)
}

/// Reads the header line, leaving `input` at the data.
fn read_header(input: &mut dyn BufRead) -> anyhow::Result<(String, VolumeBackupHeader)> {
    let mut line = String::new();
    input.read_line(&mut line).context("reading the backup")?;
    let line = line.trim_end().to_owned();
    let header = VolumeBackupHeader::from_line(&line).map_err(anyhow::Error::msg)?;
    Ok((line, header))
}

/// A file only its owner can read, created anew.
fn create_private(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))
}

pub(crate) async fn backup(
    channel: Channel,
    namespace: String,
    name: String,
    output: &Path,
) -> anyhow::Result<()> {
    let mut chunks = client(channel)
        .backup(VolumeBackupRequest {
            namespace,
            name: name.clone(),
        })
        .await?
        .into_inner();
    // Written aside and renamed once whole: a backup cut short is never
    // taken for one.
    let partial = PathBuf::from(format!("{}.partial", output.display()));
    let result = async {
        let mut file = std::io::BufWriter::new(create_private(&partial)?);
        let Some(first) = chunks.message().await? else {
            bail!("the manager sent nothing");
        };
        let header = write_header(&mut file, &first.data)?;
        let mut bytes = 0u64;
        while let Some(chunk) = chunks.message().await? {
            file.write_all(&chunk.data)?;
            bytes += chunk.data.len() as u64;
        }
        file.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        Ok((header, bytes))
    }
    .await;
    let (header, bytes) = match result {
        Ok(r) => r,
        Err(e) => {
            let _ = std::fs::remove_file(&partial);
            return Err(e);
        }
    };
    std::fs::rename(&partial, output)
        .with_context(|| format!("moving {} to {}", partial.display(), output.display()))?;
    println!(
        "backed up volume {}/{} from {} to {} ({bytes} bytes, {})",
        header.namespace,
        header.name,
        header.node,
        output.display(),
        header.consistency
    );
    Ok(())
}

pub(crate) async fn restore(
    channel: Channel,
    input: &Path,
    node: String,
    namespace: Option<String>,
    name: Option<String>,
    replace: bool,
) -> anyhow::Result<()> {
    let file =
        std::fs::File::open(input).with_context(|| format!("opening {}", input.display()))?;
    let mut reader = BufReader::with_capacity(CHUNK, file);
    let (header_json, header) = read_header(&mut reader)?;
    let start = VolumeRestoreStart {
        namespace: namespace.clone().unwrap_or_default(),
        name: name.clone().unwrap_or_default(),
        node: node.clone(),
        header_json,
        replace,
    };
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(VolumeRestoreChunk {
        part: Some(Part::Start(start)),
    })
    .await
    .ok();
    // The file's reading fails the restore by ending the stream early: the
    // node then takes nothing.
    let reading = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        let mut bytes = 0u64;
        loop {
            let mut buf = vec![0; CHUNK];
            let n = reader.read(&mut buf)?;
            if n == 0 {
                return Ok(bytes);
            }
            buf.truncate(n);
            bytes += n as u64;
            if tx
                .blocking_send(VolumeRestoreChunk {
                    part: Some(Part::Data(buf)),
                })
                .is_err()
            {
                bail!("the manager stopped taking the data");
            }
        }
    });
    let sent = client(channel)
        .restore(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await;
    let read = reading.await?;
    sent?;
    let bytes = read?;
    println!(
        "restored volume {}/{} on {node} from {} ({bytes} bytes, {} as of {})",
        namespace.as_deref().unwrap_or(&header.namespace),
        name.as_deref().unwrap_or(&header.name),
        input.display(),
        header.consistency,
        header.taken
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The header line and the data after it come back as written.
    #[test]
    fn a_backup_file_is_its_header_then_the_data() {
        let header = VolumeBackupHeader {
            format: VolumeBackupHeader::FORMAT,
            namespace: "shop".into(),
            name: "data-db-0".into(),
            labels: [("legion/service".to_owned(), "db".to_owned())].into(),
            spec: Default::default(),
            node: "n1".into(),
            taken: 1_794_000_000,
            consistency: VolumeBackupHeader::CRASH_CONSISTENT.into(),
        };
        let mut file = Vec::new();
        write_header(&mut file, &serde_json::to_vec_pretty(&header).unwrap()).unwrap();
        file.extend_from_slice(b"\x00stream\nwith lines\n");

        let mut input = &file[..];
        let (line, read) = read_header(&mut input).unwrap();
        assert_eq!(read, header);
        assert!(!line.contains('\n'));
        let mut rest = Vec::new();
        input.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"\x00stream\nwith lines\n");

        let e = read_header(&mut &b"BZh91AY&SY..."[..]).unwrap_err();
        assert!(e.to_string().contains("not a volume backup"), "{e}");
    }
}
