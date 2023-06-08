use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio_preemption::task::{
    AsyncLockTask, BusyTask, IdleTask, IntervalBusyTask, SpawnedBusyTask, SpawnedIdleTask,
    SyncLockTask, Task, TaskSet,
};

async fn tokio_sync_lock() {
    println!("tokio_sync_lock begin:");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("schedule")
        .enable_all()
        .build()
        .unwrap();

    let lock = Arc::new(std::sync::Mutex::new(()));
    let num_jobs = 3;
    let mut join_handles = Vec::with_capacity(num_jobs);
    let task0 = SyncLockTask::new(0, lock.clone(), None, Some(Duration::from_millis(2)));
    let task1 = SyncLockTask::new(
        1,
        lock,
        Some(Duration::from_millis(1)),
        Some(Duration::from_millis(0)),
    );
    let task2 = BusyTask {
        dur: Duration::from_millis(10),
    };

    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task0.run().await;
        start.elapsed()
    });
    join_handles.push(handle);
    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task1.run().await;
        start.elapsed()
    });
    join_handles.push(handle);
    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task2.run().await;
        start.elapsed()
    });
    join_handles.push(handle);

    let mut costs = Vec::with_capacity(num_jobs);
    for h in join_handles {
        let pair = h.await.unwrap();
        costs.push(pair);
    }
    for (idx, cost) in costs.into_iter().enumerate() {
        println!("task:{idx}, cost:{cost:?}");
    }

    println!("tokio_sync_lock end.\n");
}

async fn tokio_async_lock_no_preemption() {
    println!("tokio_async_lock_no_preemption begin:");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("schedule")
        .enable_all()
        .build()
        .unwrap();

    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let num_jobs = 3;
    let mut join_handles = Vec::with_capacity(num_jobs);
    let task0 = AsyncLockTask::new(
        0,
        lock.clone(),
        None,
        Some(IdleTask {
            dur: Duration::from_millis(2),
        }),
    );
    let task1 = AsyncLockTask::new(
        1,
        lock,
        Some(BusyTask {
            dur: Duration::from_millis(1),
        }),
        None,
    );
    let task2 = BusyTask {
        dur: Duration::from_millis(10),
    };

    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task0.run().await;
        start.elapsed()
    });
    join_handles.push(handle);
    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task1.run().await;
        start.elapsed()
    });
    join_handles.push(handle);
    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task2.run().await;
        start.elapsed()
    });
    join_handles.push(handle);

    let mut costs = Vec::with_capacity(num_jobs);
    for h in join_handles {
        let pair = h.await.unwrap();
        costs.push(pair);
    }
    for (idx, cost) in costs.into_iter().enumerate() {
        println!("task:{idx}, cost:{cost:?}");
    }

    println!("tokio_async_lock_no_preemption end.\n");
}

async fn tokio_async_lock_preemption() {
    println!("tokio_async_lock_preemption begin:");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("schedule")
        .enable_all()
        .build()
        .unwrap();

    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let num_jobs = 3;
    let mut join_handles = Vec::with_capacity(num_jobs);
    let task0 = AsyncLockTask::new(
        0,
        lock.clone(),
        None,
        Some(IdleTask {
            dur: Duration::from_millis(2),
        }),
    );
    let task1 = AsyncLockTask::new(
        1,
        lock,
        Some(BusyTask {
            dur: Duration::from_millis(1),
        }),
        None,
    );
    let task2 = IntervalBusyTask {
        dur: Duration::from_millis(10),
        interval: Duration::from_millis(2),
    };

    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task0.run().await;
        start.elapsed()
    });
    join_handles.push(handle);
    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task1.run().await;
        start.elapsed()
    });
    join_handles.push(handle);
    let start = Instant::now();
    let handle = runtime.spawn(async move {
        task2.run().await;
        start.elapsed()
    });
    join_handles.push(handle);

    let mut costs = Vec::with_capacity(num_jobs);
    for h in join_handles {
        let pair = h.await.unwrap();
        costs.push(pair);
    }
    for (idx, cost) in costs.into_iter().enumerate() {
        println!("task:{idx}, cost:{cost:?}");
    }

    println!("tokio_async_lock_preemption end.\n");
}

#[allow(dead_code)]
async fn tokio_hybrid_workload_schedule() {
    println!("tokio_hybrid_workload_schedule begin:");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("schedule")
        .enable_all()
        .build()
        .unwrap();

    let num_jobs = 4;
    let mut join_handles = Vec::with_capacity(num_jobs);
    let hybrid_task1 = TaskSet::new(
        0,
        vec![
            Arc::new(BusyTask {
                dur: Duration::from_millis(10),
            }),
            Arc::new(IdleTask {
                dur: Duration::from_millis(5),
            }),
            Arc::new(IdleTask {
                dur: Duration::from_millis(5),
            }),
        ],
    );
    let hybrid_task2 = TaskSet::new(
        0,
        vec![Arc::new(BusyTask {
            dur: Duration::from_millis(20),
        })],
    );

    for _ in 0..2 {
        let task = hybrid_task1.clone();
        let start = Instant::now();
        let handle = runtime.spawn(async move {
            task.run().await;
            start.elapsed()
        });
        join_handles.push(handle);
    }
    for _ in 0..2 {
        let task = hybrid_task2.clone();
        let start = Instant::now();
        let handle = runtime.spawn(async move {
            task.run().await;
            start.elapsed()
        });
        join_handles.push(handle);
    }

    let mut costs = Vec::with_capacity(num_jobs);
    for h in join_handles {
        let pair = h.await.unwrap();
        costs.push(pair);
    }
    for (idx, cost) in costs.into_iter().enumerate() {
        println!("task:{idx}, cost:{cost:?}");
    }
    println!("tokio_hybrid_workload end.\n");
}

#[allow(dead_code)]
async fn tokio_hybrid_workload_spawn_cpu_schedule() {
    println!("tokio_hybrid_workload_spawn_cpu_schedule begin:");
    let cpu_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("cpu")
            .enable_all()
            .build()
            .unwrap(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("schedule")
        .enable_all()
        .build()
        .unwrap();

    let num_jobs = 4;
    let mut join_handles = Vec::with_capacity(num_jobs);
    let hybrid_task = TaskSet::new(
        0,
        vec![
            Arc::new(SpawnedBusyTask {
                dur: Duration::from_millis(10),
                runtime: cpu_runtime.clone(),
            }),
            Arc::new(IdleTask {
                dur: Duration::from_millis(5),
            }),
            Arc::new(IdleTask {
                dur: Duration::from_millis(5),
            }),
        ],
    );
    let busy_task = SpawnedBusyTask {
        dur: Duration::from_millis(20),
        runtime: cpu_runtime.clone(),
    };

    for _ in 0..2 {
        let task = hybrid_task.clone();
        let start = Instant::now();
        let handle = runtime.spawn(async move {
            task.run().await;
            start.elapsed()
        });
        join_handles.push(handle);
    }
    for _ in 0..2 {
        let task = busy_task.clone();
        let start = Instant::now();
        let handle = runtime.spawn(async move {
            task.run().await;
            start.elapsed()
        });
        join_handles.push(handle);
    }

    let mut costs = Vec::with_capacity(num_jobs);
    for h in join_handles {
        let pair = h.await.unwrap();
        costs.push(pair);
    }
    for (idx, cost) in costs.into_iter().enumerate() {
        println!("task:{idx}, cost:{cost:?}");
    }
    println!("tokio_hybrid_workload_spawn_cpu_schedule end.\n");
}

#[allow(dead_code)]
async fn tokio_hybrid_workload_spawn_io_schedule() {
    println!("tokio_hybrid_workload_spawn_io_schedule begin:");
    let io_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("io")
            .enable_all()
            .build()
            .unwrap(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("schedule")
        .enable_all()
        .build()
        .unwrap();

    let num_jobs = 4;
    let mut join_handles = Vec::with_capacity(num_jobs);
    let hybrid_task = TaskSet::new(
        0,
        vec![
            Arc::new(BusyTask {
                dur: Duration::from_millis(10),
            }),
            Arc::new(SpawnedIdleTask {
                dur: Duration::from_millis(5),
                runtime: io_runtime.clone(),
            }),
            Arc::new(SpawnedIdleTask {
                dur: Duration::from_millis(5),
                runtime: io_runtime.clone(),
            }),
        ],
    );
    let busy_task = BusyTask {
        dur: Duration::from_millis(20),
    };

    for _ in 0..2 {
        let task = hybrid_task.clone();
        let start = Instant::now();
        let handle = runtime.spawn(async move {
            task.run().await;
            start.elapsed()
        });
        join_handles.push(handle);
    }
    for _ in 0..2 {
        let task = busy_task.clone();
        let start = Instant::now();
        let handle = runtime.spawn(async move {
            task.run().await;
            start.elapsed()
        });
        join_handles.push(handle);
    }

    let mut costs = Vec::with_capacity(num_jobs);
    for h in join_handles {
        let pair = h.await.unwrap();
        costs.push(pair);
    }

    for (idx, cost) in costs.into_iter().enumerate() {
        println!("task:{idx}, cost:{cost:?}");
    }
    println!("tokio_hybrid_workload_spawn_io_schedule end.\n");
}

fn main() {
    futures::executor::block_on(tokio_sync_lock());
    futures::executor::block_on(tokio_async_lock_no_preemption());
    futures::executor::block_on(tokio_async_lock_preemption());

    futures::executor::block_on(tokio_hybrid_workload_schedule());
    futures::executor::block_on(tokio_hybrid_workload_spawn_io_schedule());
    futures::executor::block_on(tokio_hybrid_workload_spawn_cpu_schedule());
}
