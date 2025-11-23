use octopii::OctopiiRuntime;
use std::error::Error;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let value = run_runtime_example()?;
    println!("Runtime example computed value: {}", value);
    Ok(())
}

pub fn run_runtime_example() -> Result<u32, Box<dyn Error>> {
    let runtime = OctopiiRuntime::new(2);
    let handle = runtime.spawn(async {
        tokio::time::sleep(Duration::from_millis(25)).await;
        40 + 2
    });

    let value = runtime.handle().block_on(handle)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::run_runtime_example;

    #[test]
    fn runtime_executes_tasks() {
        let value = run_runtime_example().expect("runtime example should succeed");
        assert_eq!(value, 42);
    }
}
