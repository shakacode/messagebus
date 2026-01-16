//! Demonstrates the task grouping feature of the message bus.
//!
//! This example shows how to:
//! - Define messages with group IDs using the `#[group_id]` attribute
//! - Track tasks by group
//! - Wait for all tasks in a group to complete using `flush_group`

use async_trait::async_trait;
use messagebus::derive::Message;
use messagebus::error::GenericError;
use messagebus::{AsyncHandler, Bus, GroupId};
use std::time::Duration;

/// A message representing a job that belongs to a specific job group.
/// The `#[group_id]` attribute specifies which field determines the group.
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct ProcessJob {
    job_id: i64,
    task_name: String,
}

/// Handler that processes job messages.
struct JobProcessor;

#[async_trait]
impl AsyncHandler<ProcessJob> for JobProcessor {
    type Error = GenericError;
    type Response = ();

    async fn handle(&self, msg: ProcessJob, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        // The current group_id is available via Bus::current_group_id()
        let group_id = Bus::current_group_id();
        println!(
            "Processing '{}' for job {} (group_id: {:?})",
            msg.task_name, msg.job_id, group_id
        );

        // Simulate some async work
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("Completed '{}' for job {}", msg.task_name, msg.job_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the bus with our handler
    let (bus, poller) = Bus::build()
        .register(JobProcessor)
        .subscribe_async::<ProcessJob>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    // Define two job groups
    let job_1: GroupId = 1001;
    let job_2: GroupId = 2002;

    // Send tasks for job 1
    println!("\n--- Sending tasks for job {} ---", job_1);
    bus.send(ProcessJob {
        job_id: job_1,
        task_name: "Download data".into(),
    })
    .await?;
    bus.send(ProcessJob {
        job_id: job_1,
        task_name: "Parse data".into(),
    })
    .await?;
    bus.send(ProcessJob {
        job_id: job_1,
        task_name: "Validate data".into(),
    })
    .await?;

    // Send tasks for job 2
    println!("\n--- Sending tasks for job {} ---", job_2);
    bus.send(ProcessJob {
        job_id: job_2,
        task_name: "Initialize".into(),
    })
    .await?;
    bus.send(ProcessJob {
        job_id: job_2,
        task_name: "Process".into(),
    })
    .await?;

    // Wait for job 1 to complete
    println!("\n--- Waiting for job {} to complete ---", job_1);
    bus.flush_group(job_1).await;
    println!(
        "Job {} completed! Is idle: {}",
        job_1,
        bus.is_group_idle(job_1)
    );

    // Wait for job 2 to complete
    println!("\n--- Waiting for job {} to complete ---", job_2);
    bus.flush_group(job_2).await;
    println!(
        "Job {} completed! Is idle: {}",
        job_2,
        bus.is_group_idle(job_2)
    );

    // Clean up
    bus.close().await;
    println!("\n--- Bus closed ---");

    Ok(())
}
