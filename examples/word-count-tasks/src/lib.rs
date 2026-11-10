use loom_guest_sdk::{Guest, Task, export, run_task};
use std::collections::BTreeMap;

struct CountWords;

impl Task for CountWords {
    type Input = String;
    type Output = BTreeMap<String, u64>;

    fn run(text: String) -> Result<Self::Output, String> {
        let mut counts = BTreeMap::new();
        for word in text.split_whitespace() {
            *counts.entry(word.to_owned()).or_insert(0) += 1;
        }
        Ok(counts)
    }
}

struct WordCountGuest;

impl Guest for WordCountGuest {
    fn task(name: String, input: Vec<u8>) -> Result<Vec<u8>, String> {
        match name.as_str() {
            "count_words" => run_task::<CountWords>(&input),
            _ => Err(format!("unknown task: {name}")),
        }
    }

    fn handle(_: String, _: Vec<u8>, _: Vec<u8>) -> Result<Vec<u8>, String> {
        Err("this component only exports tasks".into())
    }
}

export!(WordCountGuest with_types_in loom_guest_sdk::bindings);
