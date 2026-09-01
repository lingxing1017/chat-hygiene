use std::fs;
use std::path::Path;

fn read(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
}

#[test]
fn dockerfile_builds_locked_release_on_minimal_alpine_image() {
    let dockerfile = read("Dockerfile");
    assert!(dockerfile.contains("FROM rust:1.96-alpine3.24 AS builder"));
    assert!(dockerfile.contains("apk add --no-cache musl-dev pkgconfig"));
    assert!(dockerfile.contains("RUN cargo build --locked --release"));
    assert!(dockerfile.contains("FROM alpine:3.24 AS runtime"));
    assert!(dockerfile.contains("apk add --no-cache ca-certificates curl"));
    assert!(dockerfile.contains("COPY --from=builder"));
    assert!(dockerfile.contains("/target/release/chathygiene"));
    assert!(dockerfile.contains("EXPOSE 8080"));
    assert!(dockerfile.contains("/health/live"));
    assert!(!dockerfile.contains("/health/ready"));
    assert!(dockerfile.contains("--start-period=10m"));
    assert!(
        dockerfile
            .contains("COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh")
    );
    assert!(dockerfile.contains("ENTRYPOINT [\"/usr/local/bin/docker-entrypoint.sh\"]"));
    assert!(dockerfile.contains("CMD [\"chathygiene\"]"));
    assert!(dockerfile.contains("mkdir -m 0700 /data"));

    let runtime = dockerfile
        .split("FROM alpine:3.24 AS runtime")
        .nth(1)
        .expect("runtime stage");
    assert!(
        !runtime
            .lines()
            .any(|line| line.trim_start().starts_with("USER "))
    );
    for forbidden in [
        "cargo",
        "rustc",
        "COPY src",
        "COPY tests",
        "apt-get",
        "useradd",
        "groupadd",
    ] {
        assert!(!runtime.contains(forbidden), "runtime contains {forbidden}");
    }
}

#[test]
fn compose_runs_one_dry_run_service_with_bind_mounted_data() {
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
    assert!(compose.contains("./data:/data"));
    assert!(compose.contains("${CHATHYGIENE_PORT:-8080}:8080"));
    assert!(!compose.contains("CHATHYGIENE_PUBLIC_WEBHOOK_URL:"));
    assert!(!compose.contains("chathygiene-data"));
    assert!(!compose.contains("replicas:"));

    let example = read(".env.example");
    assert_eq!(
        example,
        "CHATHYGIENE_BOT_TOKEN=\n\
CHATHYGIENE_PUBLIC_WEBHOOK_URL=\n\
CHATHYGIENE_DESTRUCTIVE_MODE=false\n\
CHATHYGIENE_PORT=8080\n\
RUST_LOG=info\n"
    );
    for removed in [
        "CHATHYGIENE_WEBHOOK_SECRET",
        "CHATHYGIENE_CHALLENGE_HMAC_KEY",
        "CHATHYGIENE_OWNER_USER_ID",
    ] {
        assert!(!example.contains(removed));
    }

    let ignored = read(".dockerignore");
    for entry in ["target", ".git", ".env", "data", "tests"] {
        assert!(ignored.lines().any(|line| line == entry));
    }
}

#[test]
fn entrypoint_enforces_the_private_default_data_contract() {
    let entrypoint = read("docker-entrypoint.sh");
    assert!(entrypoint.starts_with("#!/bin/sh\nset -eu\n"));
    assert!(entrypoint.contains("umask 077"));
    assert!(entrypoint.contains("[ -d /data ] && [ ! -L /data ]"));
    assert!(entrypoint.contains("stat -c '%a' /data"));
    assert!(entrypoint.contains("/data/chathygiene.db-wal"));
    assert!(entrypoint.contains("/data/chathygiene.db-shm"));
    assert!(entrypoint.contains("[ -f \"$database_file\" ] && [ ! -L \"$database_file\" ]"));
    assert!(entrypoint.contains("= \"600\" ] || permission_error"));
    assert!(entrypoint.contains("exec \"$@\""));
    for forbidden in ["chmod ", "chown ", "claim-code", "CHATHYGIENE_BOT_TOKEN"] {
        assert!(!entrypoint.contains(forbidden));
    }
}

#[cfg(unix)]
mod container_acceptance {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::process::Command;

    fn image() -> String {
        std::env::var("CHATHYGIENE_TEST_IMAGE")
            .unwrap_or_else(|_| "chathygiene:managed-credentials-plan".to_owned())
    }

    fn run(directory: &std::path::Path, command: &[&str]) -> std::process::Output {
        let mut docker = Command::new("docker");
        docker
            .args(["run", "--rm", "-v"])
            .arg(format!("{}:/data", directory.display()))
            .arg(image())
            .args(command);
        docker.output().expect("run isolated container acceptance")
    }

    #[test]
    #[ignore = "requires the explicitly built local managed-credentials image"]
    fn entrypoint_masks_new_database_sidecars() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let output = run(
            directory.path(),
            &[
                "sh",
                "-c",
                "touch /data/chathygiene.db /data/chathygiene.db-wal /data/chathygiene.db-shm",
            ],
        );
        assert!(output.status.success());
        for name in ["chathygiene.db", "chathygiene.db-wal", "chathygiene.db-shm"] {
            let mode = fs::metadata(directory.path().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    #[ignore = "requires the explicitly built local managed-credentials image"]
    fn entrypoint_rejects_broad_or_symlinked_existing_state() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let database = directory.path().join("chathygiene.db");
        fs::write(&database, b"database-sentinel").unwrap();
        fs::set_permissions(&database, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!run(directory.path(), &["true"]).status.success());
        assert_eq!(fs::read(&database).unwrap(), b"database-sentinel");
        assert_eq!(
            fs::metadata(directory.path()).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert_eq!(
            fs::metadata(&database).unwrap().permissions().mode() & 0o777,
            0o644
        );

        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&database, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(run(directory.path(), &["true"]).status.success());

        fs::remove_file(&database).unwrap();
        let outside = directory.path().join("outside");
        fs::write(&outside, b"outside-sentinel").unwrap();
        symlink(&outside, &database).unwrap();
        assert!(!run(directory.path(), &["true"]).status.success());
        assert_eq!(fs::read(outside).unwrap(), b"outside-sentinel");
    }
}
