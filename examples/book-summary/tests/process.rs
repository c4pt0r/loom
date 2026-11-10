//! Process adapter tests use local executable fixtures, not a model or network.
#![cfg(unix)]
use loom_book_summary::{AgentExecutor, AgentRequest, pi::LocalPi};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("loom-pi-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn executable(&self, body: &str) -> PathBuf {
        let p = self.0.join("pi-fixture");
        std::fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o700)).unwrap();
        p
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn request() -> AgentRequest {
    AgentRequest {
        prompt: "Summarize this.".into(),
        source: "Chapter text".into(),
        model: Some("provider/model with spaces".into()),
    }
}
async fn pid_file(root: &Path) -> PathBuf {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for entry in std::fs::read_dir(root).unwrap().flatten() {
                let p = entry.path().join("pid");
                if p.is_file() {
                    return p;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
async fn wait_dead(pid: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !tokio::process::Command::new("/bin/kill")
                .args(["-0", pid])
                .stderr(std::process::Stdio::null())
                .status()
                .await
                .unwrap()
                .success()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn uses_local_executable_separate_workspace_and_literal_arguments() {
    let tmp = Temp::new();
    let exe = tmp.executable(r#"printf '%s\n' "$@" > args.txt
printf '%s\n' '{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"{}"}]}}'"#);
    let adapter = LocalPi {
        executable: exe,
        work_dir: tmp.0.clone(),
        timeout: Duration::from_secs(5),
    };
    assert_eq!(adapter.run(request()).await.unwrap(), "{}");
    assert_eq!(adapter.run(request()).await.unwrap(), "{}");
    let dirs: Vec<_> = std::fs::read_dir(&tmp.0)
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_dir())
        .collect();
    assert_eq!(dirs.len(), 2);
    for dir in dirs {
        let args = std::fs::read_to_string(dir.path().join("args.txt")).unwrap();
        assert!(args.contains("--model\nprovider/model with spaces\n"));
        assert!(args.contains("--no-mcp\n"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("source.txt")).unwrap(),
            "Chapter text"
        );
    }
}
#[tokio::test]
async fn timeout_reaps_local_process() {
    let tmp = Temp::new();
    let exe = tmp.executable("echo $$ > pid\nexec sleep 30");
    let adapter = LocalPi {
        executable: exe,
        work_dir: tmp.0.clone(),
        timeout: Duration::from_millis(300),
    };
    let running = tokio::spawn(async move { adapter.run(request()).await });
    let path = pid_file(&tmp.0).await;
    let pid = std::fs::read_to_string(path).unwrap();
    assert!(
        running
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    wait_dead(pid.trim()).await;
}
#[tokio::test]
async fn dropping_execution_kills_local_process() {
    let tmp = Temp::new();
    let exe = tmp.executable("echo $$ > pid\nexec sleep 30");
    let adapter = LocalPi {
        executable: exe,
        work_dir: tmp.0.clone(),
        timeout: Duration::from_secs(30),
    };
    let running = tokio::spawn(async move { adapter.run(request()).await });
    let path = pid_file(&tmp.0).await;
    let pid = std::fs::read_to_string(path).unwrap();
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    wait_dead(pid.trim()).await;
}
