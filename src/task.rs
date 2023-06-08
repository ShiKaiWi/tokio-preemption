use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;

pub type TaskRef = Arc<dyn Task + Send + Sync>;

#[async_trait]
pub trait Task {
    fn name(&self) -> &str;
    async fn run(&self) -> Duration;
}

#[derive(Clone)]
pub struct SpawnedBusyTask {
    pub dur: Duration,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[async_trait]
impl Task for SpawnedBusyTask {
    fn name(&self) -> &str {
        "spawned_busy_task"
    }

    async fn run(&self) -> Duration {
        let busy_task = BusyTask { dur: self.dur };
        self.runtime
            .spawn(async move { busy_task.run().await })
            .await
            .unwrap()
    }
}

#[derive(Clone)]
pub struct BusyTask {
    pub dur: Duration,
}

impl BusyTask {
    fn sync_run(&self) -> Duration {
        let start = Instant::now();
        let mut n = 0;
        loop {
            if n % 10000 == 0 {
                if start.elapsed() > self.dur {
                    break;
                }
                n = 0;
            }
            n += 1;
        }

        start.elapsed()
    }
}

#[async_trait]
impl Task for BusyTask {
    fn name(&self) -> &str {
        "busy_task"
    }

    async fn run(&self) -> Duration {
        self.sync_run()
    }
}

pub struct IntervalBusyTask {
    pub dur: Duration,
    pub interval: Duration,
}

#[async_trait]
impl Task for IntervalBusyTask {
    fn name(&self) -> &str {
        "interval_busy_task"
    }

    async fn run(&self) -> Duration {
        let start = Instant::now();
        let mut next_idle_elapsed = self.interval;
        let mut n = 0;
        loop {
            if n % 10000 == 0 {
                if start.elapsed() > self.dur {
                    break;
                }
                if start.elapsed() > next_idle_elapsed {
                    tokio::time::sleep(Duration::from_micros(0)).await;
                    next_idle_elapsed += self.interval;
                }
                n = 0;
            }
            n += 1;
        }

        start.elapsed()
    }
}

pub struct SpawnedIdleTask {
    pub dur: Duration,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[async_trait]
impl Task for SpawnedIdleTask {
    fn name(&self) -> &str {
        "spawned_idle_task"
    }

    async fn run(&self) -> Duration {
        let task = IdleTask { dur: self.dur };
        self.runtime
            .spawn(async move { task.run().await })
            .await
            .unwrap()
    }
}

pub struct IdleTask {
    pub dur: Duration,
}

#[async_trait]
impl Task for IdleTask {
    fn name(&self) -> &str {
        "idle_task"
    }

    async fn run(&self) -> Duration {
        tokio::time::sleep(self.dur).await;
        self.dur
    }
}

#[derive(Clone)]
pub struct SyncLockTask {
    name: String,
    lock: Arc<Mutex<()>>,
    before_hold_dur: Option<Duration>,
    hold_dur: Option<Duration>,
}

impl SyncLockTask {
    pub fn new(
        id: usize,
        lock: Arc<Mutex<()>>,
        before_hold_dur: Option<Duration>,
        hold_dur: Option<Duration>,
    ) -> Self {
        Self {
            name: format!("sync_lock_task#{id}"),
            lock,
            before_hold_dur,
            hold_dur,
        }
    }
}

#[async_trait]
impl Task for SyncLockTask {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self) -> Duration {
        let start = Instant::now();
        if let Some(dur) = self.before_hold_dur {
            BusyTask { dur }.sync_run();
        }

        let _guard = self.lock.lock().unwrap();
        if let Some(dur) = self.hold_dur {
            BusyTask { dur }.sync_run();
        }
        start.elapsed()
    }
}

pub struct AsyncLockTask<T> {
    name: String,
    lock: Arc<tokio::sync::Mutex<()>>,
    before_task: Option<T>,
    after_task: Option<T>,
}

impl<T> AsyncLockTask<T> {
    pub fn new(
        id: usize,
        lock: Arc<tokio::sync::Mutex<()>>,
        before_task: Option<T>,
        after_task: Option<T>,
    ) -> Self {
        Self {
            name: format!("async_lock_task#{id}"),
            lock,
            before_task,
            after_task,
        }
    }
}

#[async_trait]
impl<T: Task + Send + Sync> Task for AsyncLockTask<T> {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self) -> Duration {
        let start = Instant::now();
        if let Some(t) = &self.before_task {
            t.run().await;
        }
        let _guard = self.lock.lock().await;
        if let Some(t) = &self.after_task {
            t.run().await;
        }
        start.elapsed()
    }
}

#[derive(Clone)]
pub struct TaskSet {
    name: String,
    tasks: Vec<TaskRef>,
}

#[async_trait]
impl Task for TaskSet {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self) -> Duration {
        let mut total_duration = Duration::from_millis(0);
        for task in &self.tasks {
            total_duration += task.run().await;
        }
        total_duration
    }
}

impl TaskSet {
    pub fn new(id: usize, tasks: Vec<TaskRef>) -> Self {
        Self {
            name: format!("task-set-{id}"),
            tasks,
        }
    }
}
