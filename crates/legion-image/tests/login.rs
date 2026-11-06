//! `Client::check_login` against a small registry on plain HTTP.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

use legion_image::{Client, Credentials};

/// Answers each request with what `reply` makes of its first line and its
/// `Authorization`: a status line and headers. Returns the registry's name.
fn registry(reply: impl Fn(&str, Option<&str>, &str) -> String + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let name = addr.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut read = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            read.read_line(&mut first).unwrap();
            let mut auth = None;
            loop {
                let mut line = String::new();
                read.read_line(&mut line).unwrap();
                if line.trim().is_empty() {
                    break;
                }
                if let Some((k, v)) = line.split_once(':')
                    && k.eq_ignore_ascii_case("authorization")
                {
                    auth = Some(v.trim().to_owned());
                }
            }
            let head = reply(first.trim(), auth.as_deref(), &addr);
            let text = format!("{head}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(text.as_bytes());
        }
    });
    name
}

fn client(registry: &str) -> Client {
    Client::new().unwrap().plain_http(registry)
}

fn bob(password: &str) -> Credentials {
    Credentials {
        username: "bob".into(),
        password: password.into(),
    }
}

/// "bob:right" in base64.
const RIGHT: &str = "Basic Ym9iOnJpZ2h0";

#[tokio::test]
async fn a_login_is_checked_at_the_registrys_auth_service() {
    let r = registry(|first, auth, addr| {
        if first.starts_with("GET /v2/ ") {
            format!(
                "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer \
                 realm=\"http://{addr}/token\",service=\"reg\""
            )
        } else if first.contains("account=bob") && first.contains("service=reg") {
            if auth == Some(RIGHT) {
                "HTTP/1.1 200 OK".to_owned()
            } else {
                "HTTP/1.1 401 Unauthorized".to_owned()
            }
        } else {
            "HTTP/1.1 400 Bad Request".to_owned()
        }
    });
    let c = client(&r);
    c.check_login(&r, &bob("right")).await.unwrap();
    let err = c.check_login(&r, &bob("wrong")).await.unwrap_err();
    assert!(err.to_string().contains("login refused for bob"), "{err}");
}

#[tokio::test]
async fn a_login_is_checked_on_a_basic_registry() {
    let r = registry(|_, auth, _| match auth {
        Some(RIGHT) => "HTTP/1.1 200 OK".to_owned(),
        _ => "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"reg\"".to_owned(),
    });
    let c = client(&r);
    c.check_login(&r, &bob("right")).await.unwrap();
    assert!(c.check_login(&r, &bob("wrong")).await.is_err());
}

#[tokio::test]
async fn a_registry_asking_for_nothing_takes_any_login() {
    let r = registry(|_, _, _| "HTTP/1.1 200 OK".to_owned());
    client(&r).check_login(&r, &bob("any")).await.unwrap();
}

/// The login goes to the realm, so one in the clear from a registry not
/// spoken to in the clear never gets it.
#[tokio::test]
async fn a_login_goes_to_no_realm_in_the_clear() {
    let c = Client::new().unwrap();
    let err = c
        .check_realm("registry.example.org", "http://auth.example.org/token")
        .unwrap_err();
    assert!(err.to_string().contains("not https"), "{err}");
    c.check_realm("registry.example.org", "https://auth.example.org/token")
        .unwrap();
    let c = c.plain_http("registry.example.org");
    c.check_realm("registry.example.org", "http://auth.example.org/token")
        .unwrap();
    assert!(
        c.check_realm("registry.example.org", "ftp://auth.example.org/token")
            .is_err()
    );
}
