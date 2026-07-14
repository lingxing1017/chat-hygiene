use std::path::Path;

use tempfile::TempDir;

pub fn temporary_database() -> (TempDir, String) {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let database_path = directory.path().join("chathygiene.db");
    let url = sqlite_url(&database_path);
    (directory, url)
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}
