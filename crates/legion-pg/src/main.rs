//! Legion's PostgreSQL replication plugin (docs/REPLICATION.md, SPI v1):
//! `legion-replication-postgres <operation>`, the request as JSON on
//! standard input, the response as JSON on standard output. Run by the
//! agent in the database container's jail, as its user, it drives the
//! image's own PostgreSQL tools.

mod pg;

use std::io::Read;
use std::process::ExitCode;

fn main() -> ExitCode {
    let operation = std::env::args().nth(1).unwrap_or_default();
    // PostgreSQL's archive_command, not Legion's: `archive-wal PATH NAME
    // SPOOL`, nothing on standard input.
    if operation == "archive-wal" {
        let args: Vec<String> = std::env::args().skip(2).collect();
        let [path, name, spool] = args.as_slice() else {
            eprintln!("usage: archive-wal PATH NAME SPOOL");
            return ExitCode::FAILURE;
        };
        return match pg::archive_wal(
            std::path::Path::new(path),
            name,
            std::path::Path::new(spool),
        ) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        };
    }
    let mut input = String::new();
    let result = match std::io::stdin().read_to_string(&mut input) {
        Ok(_) => pg::handle(&operation, &input, &mut pg::System),
        Err(e) => Err(pg::Fail::new(format!("reading the request: {e}"))),
    };
    match result {
        Ok(response) => {
            println!("{response}");
            ExitCode::SUCCESS
        }
        Err(f) => {
            eprintln!("{operation}: {}", f.message);
            println!(
                "{}",
                serde_json::json!({ "error": { "code": f.code, "message": f.message } })
            );
            ExitCode::FAILURE
        }
    }
}
