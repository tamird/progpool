use std::collections::BTreeSet;
use std::convert::TryInto as _;
use std::sync::Mutex;
use std::time::Instant;

fn progress_bar_style(task_count: usize) -> indicatif::ProgressStyle {
    let task_count_digits = task_count.to_string().len();
    let count = "{pos:>".to_owned() + &(8 - task_count_digits).to_string() + "}/{len}";
    let template =
        "[{elapsed_precise}] {prefix} ".to_owned() + &count + " {bar:40.cyan/blue}: {msg}";
    indicatif::ProgressStyle::default_bar()
        .template(&template)
        .progress_chars("##-")
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct TaskMonitorId((Instant, String, usize));

struct TaskMonitor {
    counter: usize,
    tasks: BTreeSet<TaskMonitorId>,
    progress_bar: indicatif::ProgressBar,
}

impl TaskMonitor {
    fn new(progress_bar: indicatif::ProgressBar) -> TaskMonitor {
        TaskMonitor {
            counter: 0,
            tasks: BTreeSet::new(),
            progress_bar,
        }
    }

    fn started(&mut self, task_name: String) -> TaskMonitorId {
        let now = Instant::now();
        let task_id = TaskMonitorId((now, task_name, self.counter));
        self.counter += 1;
        self.tasks.insert(task_id.clone());
        self.update_progress_bar();
        task_id
    }

    fn finished(&mut self, task_id: TaskMonitorId) {
        let removed = self.tasks.remove(&task_id);
        assert!(removed);
        self.progress_bar.inc(1);
        self.update_progress_bar();
    }

    fn update_progress_bar(&mut self) {
        match self.tasks.iter().next() {
            Some(task) => {
                self.progress_bar.set_message((task.0).1.clone());
            }

            None => {
                self.progress_bar.set_message("");
            }
        }
    }
}

pub struct Pool {
    thread_count: Option<usize>,
    thread_pool: Option<rayon::ThreadPool>,
    quiet: bool,
}

pub struct ExecutionResult<T> {
    pub name: String,
    pub result: T,
}

pub struct ExecutionResults<T, E> {
    pub successful: Vec<ExecutionResult<T>>,
    pub failed: Vec<ExecutionResult<E>>,
}

impl Pool {
    pub fn with_default_size() -> Pool {
        Pool {
            thread_count: None,
            thread_pool: None,
            quiet: false,
        }
    }

    pub fn with_size(thread_count: usize) -> Pool {
        Pool {
            thread_count: Some(thread_count),
            thread_pool: None,
            quiet: false,
        }
    }

    pub fn quiet(&mut self, quiet: bool) {
        self.quiet = quiet;
    }

    fn thread_pool(&mut self) -> &mut rayon::ThreadPool {
        if self.thread_pool.is_some() {
            self.thread_pool.as_mut().unwrap()
        } else {
            let builder = rayon::ThreadPoolBuilder::new();
            let builder = match self.thread_count {
                Some(count) => builder.num_threads(count),
                None => builder,
            };

            self.thread_pool = Some(
                builder
                    .build()
                    .unwrap_or_else(|err| panic!("failed to create job pool: {}", err)),
            );
            self.thread_pool.as_mut().unwrap()
        }
    }

    pub fn execute<T: Send, E: Send>(&mut self, mut job: Job<T, E>) -> ExecutionResults<T, E> {
        let mut task_monitor = TaskMonitor::new(if self.quiet {
            indicatif::ProgressBar::hidden()
        } else {
            let task_count = job.tasks.len();
            let pb = indicatif::ProgressBar::new(task_count.try_into().unwrap());
            pb.set_style(progress_bar_style(task_count));
            pb.set_prefix(job.name.clone());
            pb.enable_steady_tick(1000);
            pb
        });

        tracing::span!(tracing::Level::INFO, "Pool::execute", name = job.name).in_scope(|| {
            let mut successful = Vec::new();
            let mut failed = Vec::new();
            {
                let state = Mutex::new((&mut task_monitor, &mut successful, &mut failed));
                self.thread_pool().scope(|scope| {
                    for task in job.tasks.drain(..) {
                        scope.spawn(|_| {
                            tracing::span!(tracing::Level::INFO, "Task::task", name = task.name)
                                .in_scope(|| {
                                    let task_id = {
                                        let mut guard = state.lock().unwrap();
                                        let (task_monitor, _, _) = &mut *guard;
                                        task_monitor.started(task.name.clone())
                                    };

                                    let result = (task.task)();

                                    let mut guard = state.lock().unwrap();
                                    let (task_monitor, successful, failed) = &mut *guard;
                                    task_monitor.finished(task_id);
                                    match result {
                                        Ok(result) => {
                                            successful.push(ExecutionResult {
                                                name: task.name,
                                                result,
                                            });
                                        }

                                        Err(err) => failed.push(ExecutionResult {
                                            name: task.name,
                                            result: err,
                                        }),
                                    }
                                })
                        })
                    }
                });
            }
            task_monitor.progress_bar.finish();

            ExecutionResults { successful, failed }
        })
    }
}

pub struct Job<'task, T: Send, E: Send> {
    name: String,
    tasks: Vec<Task<'task, T, E>>,
}

impl<'task, T: Send, E: Send> Job<'task, T, E> {
    pub fn with_name<N: Into<String>>(name: N) -> Self {
        Job {
            name: name.into(),
            tasks: Vec::new(),
        }
    }

    pub fn add_task<N: Into<String>, F: FnOnce() -> Result<T, E> + Send + 'task>(
        &mut self,
        name: N,
        task: F,
    ) {
        let name = name.into();
        self.tasks.push(Task {
            name,
            task: Box::new(task),
        });
    }
}

struct Task<'task, T: Send, E: Send> {
    name: String,
    task: Box<dyn FnOnce() -> Result<T, E> + Send + 'task>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        use super::*;

        let (send_1, recv_1) = std::sync::mpsc::channel();
        let (send_2, recv_2) = std::sync::mpsc::channel();
        let (send_3, recv_3) = std::sync::mpsc::channel();

        let mut job = Job::with_name("forward");
        job.add_task("first", move || {
            let data = recv_1.recv().unwrap();
            send_2.send(data + 1).unwrap();
            Ok(data)
        });

        job.add_task("second", move || {
            let data = recv_2.recv().unwrap();
            send_3.send(data + 1).unwrap();
            Ok(data)
        });

        job.add_task("third", move || {
            let data = recv_3.recv().unwrap();
            Err(format!("{}", data + 1))
        });

        send_1.send(0).unwrap();

        // Dispatch order is not guaranteed, must schedule all at once to avoid deadlock.
        let mut pool = Pool::with_size(job.tasks.len());
        let results = pool.execute(job);

        assert_eq!(results.successful.len(), 2);
        assert_eq!(results.successful[0].name, "first");
        assert_eq!(results.successful[0].result, 0);

        assert_eq!(results.successful[1].name, "second");
        assert_eq!(results.successful[1].result, 1);

        assert_eq!(results.failed.len(), 1);
        assert_eq!(results.failed[0].name, "third");
        assert_eq!(format!("{}", results.failed[0].result), format!("{}", 3));
    }
}
