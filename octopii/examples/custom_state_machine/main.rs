use bytes::Bytes;
use octopii::StateMachineTrait;
use std::error::Error;
use std::sync::RwLock;

fn main() -> Result<(), Box<dyn Error>> {
    let outputs = run_counter_demo()?;
    println!("Counter state machine outputs: {:?}", outputs);
    Ok(())
}

struct CounterStateMachine {
    counter: RwLock<i64>,
}

impl CounterStateMachine {
    fn new() -> Self {
        Self {
            counter: RwLock::new(0),
        }
    }
}

impl StateMachineTrait for CounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd = std::str::from_utf8(command).map_err(|e| e.to_string())?;
        match cmd {
            "INCREMENT" => {
                let mut guard = self.counter.write().unwrap();
                *guard += 1;
                Ok(Bytes::from(guard.to_string()))
            }
            "DECREMENT" => {
                let mut guard = self.counter.write().unwrap();
                *guard -= 1;
                Ok(Bytes::from(guard.to_string()))
            }
            "GET" => {
                let guard = self.counter.read().unwrap();
                Ok(Bytes::from(guard.to_string()))
            }
            "RESET" => {
                let mut guard = self.counter.write().unwrap();
                *guard = 0;
                Ok(Bytes::from("0"))
            }
            other => Err(format!("Unknown command: {}", other)),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        self.counter.read().unwrap().to_le_bytes().to_vec()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        if data.is_empty() {
            return Ok(());
        }
        if data.len() != 8 {
            return Err("snapshot should be 8 bytes".into());
        }
        let value = i64::from_le_bytes(data.try_into().unwrap());
        *self.counter.write().unwrap() = value;
        Ok(())
    }
}

pub fn run_counter_demo() -> Result<Vec<String>, Box<dyn Error>> {
    let counter = CounterStateMachine::new();
    let mut outputs = Vec::new();

    for cmd in ["INCREMENT", "INCREMENT", "DECREMENT", "GET"] {
        let out = counter.apply(cmd.as_bytes())?;
        outputs.push(String::from_utf8(out.to_vec())?);
    }

    let snapshot = counter.snapshot();
    counter.apply(b"RESET")?;
    counter.restore(&snapshot)?;
    let restored = counter.apply(b"GET")?;
    outputs.push(String::from_utf8(restored.to_vec())?);

    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::run_counter_demo;

    #[test]
    fn counter_state_machine_behaves() {
        let outputs = run_counter_demo().expect("counter demo should run");
        assert_eq!(outputs, vec!["1", "2", "1", "1", "1"]);
    }
}
