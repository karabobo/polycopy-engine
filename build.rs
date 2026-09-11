use std::{env, fs, path::Path, process::Command};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Cargo must set CARGO_MANIFEST_DIR");
    let sources = [
        "src/canary.rs",
        "src/canary_run.rs",
        "src/copytrading/prepare.rs",
        "src/venue/execution_contract.rs",
    ];

    let mut fingerprint = FNV_OFFSET_BASIS;
    for source in sources {
        println!("cargo:rerun-if-changed={source}");
        fingerprint_bytes(&mut fingerprint, source.as_bytes());
        let path = Path::new(&manifest_dir).join(source);
        let contents = fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "unable to read construction-contract source {}: {error}",
                path.display()
            )
        });
        fingerprint_bytes(&mut fingerprint, &contents);
    }
    println!("cargo:rustc-env=POLYCOPY_CONSTRUCTION_FINGERPRINT=fnv1a64-v1-{fingerprint:016x}");

    println!("cargo:rerun-if-env-changed=POLYCOPY_RELEASE_COMMIT");
    let git_commit = env::var("POLYCOPY_RELEASE_COMMIT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(git_head_commit)
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=POLYCOPY_BUILD_GIT_COMMIT={git_commit}");
}

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001b3;

fn fingerprint_bytes(fingerprint: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *fingerprint ^= u64::from(*byte);
        *fingerprint = fingerprint.wrapping_mul(FNV_PRIME);
    }
}

fn git_head_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?;
    let commit = commit.trim();
    (!commit.is_empty()).then(|| commit.to_owned())
}
