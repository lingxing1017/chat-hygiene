use std::fs;
use std::path::Path;

fn read(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
}

#[test]
fn dockerfile_builds_locked_release_and_runs_minimal_non_root_image() {
    let dockerfile = read("Dockerfile");
    assert!(dockerfile.contains("FROM rust:1.96-bookworm AS builder"));
    assert!(dockerfile.contains("RUN cargo build --locked --release"));
    assert!(dockerfile.contains("FROM debian:bookworm-slim AS runtime"));
    assert!(dockerfile.contains("ca-certificates curl"));
    assert!(dockerfile.contains("USER chathygiene"));
    assert!(dockerfile.contains("COPY --from=builder"));
    assert!(dockerfile.contains("/target/release/chathygiene"));
    assert!(dockerfile.contains("EXPOSE 8080"));
    assert!(dockerfile.contains("/health/ready"));
    let runtime = dockerfile
        .split("FROM debian:bookworm-slim AS runtime")
        .nth(1)
        .expect("runtime stage");
    for forbidden in ["cargo", "rustc", "COPY src", "COPY tests", "apt cache"] {
        assert!(!runtime.contains(forbidden), "runtime contains {forbidden}");
    }
}

#[test]
fn compose_runs_one_dry_run_service_with_persistent_data() {
    let compose = read("compose.yml");
    assert_eq!(
        compose
            .lines()
            .filter(|line| line.starts_with("  chathygiene:"))
            .count(),
        1
    );
    assert!(compose.contains("restart: unless-stopped"));
    assert!(compose.contains("path: .env"));
    assert!(
        compose.contains("CHATHYGIENE_DESTRUCTIVE_MODE: ${CHATHYGIENE_DESTRUCTIVE_MODE:-false}")
    );
    assert!(compose.contains("chathygiene-data:/data"));
    assert!(compose.contains("chathygiene-data:"));
    assert!(!compose.contains("replicas:"));

    let example = read(".env.example");
    assert!(example.contains("CHATHYGIENE_DESTRUCTIVE_MODE=false"));
    for secret in [
        "CHATHYGIENE_BOT_TOKEN=",
        "CHATHYGIENE_WEBHOOK_SECRET=",
        "CHATHYGIENE_CHALLENGE_HMAC_KEY=",
    ] {
        let line = example
            .lines()
            .find(|line| line.starts_with(secret))
            .expect("secret placeholder");
        assert_eq!(line, secret);
    }

    let ignored = read(".dockerignore");
    for entry in ["target", ".git", ".env", "data", "tests"] {
        assert!(ignored.lines().any(|line| line == entry));
    }
}
