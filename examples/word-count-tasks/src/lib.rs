use loom_guest_sdk::{
    Guest, Reducer, ShuffleRecord, Task, decode, encode, export, record, run_reducer, run_task,
};
use std::collections::BTreeMap;

struct CountWords;

impl Task for CountWords {
    type Input = String;
    type Output = Vec<ShuffleRecord>;

    fn run(text: String) -> Result<Self::Output, String> {
        let mut counts = BTreeMap::<String, u64>::new();
        for word in text.split_whitespace() {
            *counts.entry(word.to_owned()).or_insert(0) += 1;
        }
        counts
            .into_iter()
            .map(|(key, value)| record(key.as_bytes(), &value))
            .collect()
    }
}

struct SumCounts;
impl Reducer for SumCounts {
    type State = BTreeMap<String, u64>;
    fn fold(mut state: Self::State, records: Vec<ShuffleRecord>) -> Result<Self::State, String> {
        for record in records {
            let word = String::from_utf8(record.key).map_err(|e| e.to_string())?;
            let count: u64 = decode(&record.value)?;
            let total = state.entry(word).or_default();
            *total = total.checked_add(count).ok_or("word count overflow")?;
        }
        Ok(state)
    }
}

struct WordCountGuest;

impl Guest for WordCountGuest {
    fn task(name: String, input: Vec<u8>) -> Result<Vec<u8>, String> {
        match name.as_str() {
            "count_words" => run_task::<CountWords>(&input),
            "count_words_text" => {
                let text = String::from_utf8(input).map_err(|e| e.to_string())?;
                encode(&CountWords::run(text)?)
            }
            "sum_counts" => run_reducer::<SumCounts>(&input),
            _ => Err(format!("unknown task: {name}")),
        }
    }

    fn handle(_: String, _: Vec<u8>, _: Vec<u8>) -> Result<Vec<u8>, String> {
        Err("this component only exports tasks".into())
    }
}

export!(WordCountGuest with_types_in loom_guest_sdk::bindings);
