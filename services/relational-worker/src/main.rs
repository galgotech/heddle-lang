mod executor;
mod shm;

use std::collections::HashMap;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;
use tokio_stream;
use crate::executor::PrqlExecutor;
use crate::shm::ShmManager;

#[derive(Serialize, Deserialize, Debug)]
struct WorkerRegistration {
    worker_id: String,
    address: String,
    tags: HashMap<String, String>,
    runtime: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Heartbeat {
    worker_id: String,
    timestamp: String,
    status: String,
    load: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct StepInstruction {
    id: String,
    definition_name: String,
    call: Vec<String>,
    config: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Task {
    id: String,
    step: StepInstruction,
    input_handle: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskUpdate {
    task_id: String,
    status: String,
    error: Option<String>,
    output_handle: Option<String>,
    timestamp: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cp_addr = std::env::var("HEDDLE_CP_ADDR").unwrap_or_else(|_| "http://localhost:50051".to_string());
    let worker_id = format!("relational-worker-{}", uuid::Uuid::new_v4());

    println!("Starting Relational Worker: {}", worker_id);

    let channel = Channel::from_shared(cp_addr)?.connect().await?;
    let mut client = FlightServiceClient::new(channel);

    // 1. Register Worker
    let mut tags = HashMap::new();
    tags.insert("capability".to_string(), "prql".to_string());

    let reg = WorkerRegistration {
        worker_id: worker_id.clone(),
        address: "localhost:0".to_string(),
        tags,
        runtime: "rust".to_string(),
    };

    let body = serde_json::to_vec(&reg)?;
    let action = Action {
        r#type: "register-worker".to_string(),
        body: body.into(),
    };

    let mut stream = client.do_action(action).await?.into_inner();
    if let Some(_) = stream.message().await? {
        println!("Worker registered successfully");
    }

    // 2. Start Heartbeat
    let hb_client = client.clone();
    let hb_worker_id = worker_id.clone();
    tokio::spawn(async move {
        let mut client = hb_client;
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let hb = Heartbeat {
                worker_id: hb_worker_id.clone(),
                timestamp: chrono::Utc::now().to_rfc3339(),
                status: "idle".to_string(),
                load: 0.1,
            };
            if let Ok(body) = serde_json::to_vec(&hb) {
                let action = Action {
                    r#type: "heartbeat".to_string(),
                    body: body.into(),
                };
                let _ = client.do_action(action).await;
            }
        }
    });

    // 3. Execution Loop
    let mut exchange_stream = client.do_exchange(tokio_stream::iter(Vec::<FlightData>::new())).await?.into_inner();
    
    let executor = PrqlExecutor::new();
    let shm = ShmManager::new("/dev/shm/heddle");

    println!("Execution loop started");

    while let Some(data) = exchange_stream.message().await? {
        let task: Task = serde_json::from_slice(&data.data_body)?;
        println!("Received task: {} ({})", task.id, task.step.definition_name);

        let mut update = TaskUpdate {
            task_id: task.id.clone(),
            status: "completed".to_string(),
            error: None,
            output_handle: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };

        // Check if it's a PRQL task
        if task.step.call == vec!["std", "relational", "prql"] {
            if let Some(query) = task.step.config.get("query").and_then(|v| v.as_str()) {
                if let Some(handle) = task.input_handle {
                    match shm.get(&handle) {
                        Ok(input_batch) => {
                            match executor.execute(query, input_batch).await {
                                Ok(output_batch) => {
                                    let output_handle = format!("shm-rust-{}-{}", task.id, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
                                    if let Err(e) = shm.put(&output_handle, &output_batch) {
                                        update.status = "failed".to_string();
                                        update.error = Some(format!("failed to write output to shm: {}", e));
                                    } else {
                                        update.output_handle = Some(output_handle);
                                    }
                                }
                                Err(e) => {
                                    update.status = "failed".to_string();
                                    update.error = Some(format!("PRQL execution failed: {}", e));
                                }
                            }
                        }
                        Err(e) => {
                            update.status = "failed".to_string();
                            update.error = Some(format!("failed to read input from shm: {}", e));
                        }
                    }
                }
            }
        }

        // Send update
        println!("Task {} finished with status: {}", update.task_id, update.status);
    }

    Ok(())
}
