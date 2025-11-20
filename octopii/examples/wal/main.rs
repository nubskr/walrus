use bytes::Bytes;
use octopii::wal::WriteAheadLog;
use std::error::Error;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let entries = run_wal_example().await?;
    println!("WAL replayed entries: {:?}", entries);
    Ok(())
}

pub async fn run_wal_example() -> Result<Vec<String>, Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let wal_path = data_dir.path().join("demo");
    let wal = WriteAheadLog::new(wal_path, 8, Duration::from_millis(0)).await?;

    wal.append(Bytes::from("alpha")).await?;
    wal.append(Bytes::from("beta")).await?;
    wal.flush().await?;

    let entries = wal
        .read_all()
        .await?
        .into_iter()
        .map(|b| String::from_utf8(b.to_vec()).expect("bytes are utf-8"))
        .collect();

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::run_wal_example;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_round_trip() {
        let entries = run_wal_example().await.expect("wal example should run");
        assert_eq!(entries, vec!["alpha".to_string(), "beta".to_string()]);
    }
}
