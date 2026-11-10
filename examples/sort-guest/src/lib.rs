use loom_guest_sdk::{Guest, Task, export, run_task};

struct SortChunk;

impl Task for SortChunk {
    type Input = Vec<i64>;
    type Output = Vec<i64>;

    fn run(mut numbers: Vec<i64>) -> Result<Vec<i64>, String> {
        numbers.sort_unstable();
        Ok(numbers)
    }
}

struct SortGuest;

impl Guest for SortGuest {
    fn task(name: String, input: Vec<u8>) -> Result<Vec<u8>, String> {
        match name.as_str() {
            "sort_chunk" => run_task::<SortChunk>(&input),
            _ => Err(format!("unknown task: {name}")),
        }
    }

    fn handle(_: String, _: Vec<u8>, _: Vec<u8>) -> Result<Vec<u8>, String> {
        Err("this component only exports tasks".into())
    }
}

export!(SortGuest with_types_in loom_guest_sdk::bindings);
