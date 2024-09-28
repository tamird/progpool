use std::borrow::Cow;
use std::collections::BTreeMap;
use std::convert::TryInto as _;
use std::sync::Mutex;
use std::time::Duration;

fn progress_bar_style(task_count: usize) -> indicatif::ProgressStyle {
    let task_count_digits = task_count.checked_ilog10().unwrap_or(0) + 1;
    let template = format!(
        "[{{elapsed_precise}}] {{prefix}} {{pos:>{}}}/{{len}} {{bar:40.cyan/blue}}: {{msg}}",
        8 - task_count_digits
    );
    indicatif::ProgressStyle::with_template(&template)
        .unwrap()
        .progress_chars("##-")
}

struct TaskMonitor<'task> {
    tasks: BTreeMap<usize, Cow<'task, str>>,
    progress_bar: indicatif::ProgressBar,
}

impl<'task> TaskMonitor<'task> {
    fn new(progress_bar: indicatif::ProgressBar) -> Self {
        TaskMonitor {
            tasks: Default::default(),
            progress_bar,
        }
    }

    fn started(&mut self, task_name: Cow<'task, str>) -> usize {
        let task_id = self
            .tasks
            .last_key_value()
            .map(|(task_id, _)| *task_id + 1)
            .unwrap_or_default();
        assert_eq!(self.tasks.insert(task_id, task_name), None);
        self.update_progress_bar();
        task_id
    }

    fn finished(&mut self, task_id: usize) -> Cow<'task, str> {
        let task_name = self.tasks.remove(&task_id).unwrap();
        self.progress_bar.inc(1);
        self.update_progress_bar();
        task_name
    }

    fn update_progress_bar(&mut self) {
        let msg = self
            .tasks
            .iter()
            .next()
            .map(|(_, task_name)| task_name.to_string());
        match msg {
            Some(msg) => self.progress_bar.set_message(msg),
            None => self.progress_bar.set_message(""),
        }
    }
}

pub struct Pool {
    thread_count: Option<usize>,
    thread_pool: Option<rayon::ThreadPool>,
    quiet: bool,
}

pub struct ExecutionResult<'task, T> {
    pub name: Cow<'task, str>,
    pub result: T,
}

pub struct ExecutionResults<'task, T, E> {
    pub successful: Vec<ExecutionResult<'task, T>>,
    pub failed: Vec<ExecutionResult<'task, E>>,
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
        let thread_count = self.thread_count;
        self.thread_pool.get_or_insert_with(|| {
            let builder = rayon::ThreadPoolBuilder::new();
            let builder = match thread_count {
                Some(count) => builder.num_threads(count),
                None => builder,
            };

            builder
                .build()
                .unwrap_or_else(|err| panic!("failed to create job pool: {}", err))
        })
    }

    pub fn execute<'task, T: Send, E: Send>(
        &mut self,
        mut job: Job<'task, T, E>,
    ) -> ExecutionResults<'task, T, E> {
        let mut task_monitor = TaskMonitor::new(if self.quiet {
            indicatif::ProgressBar::hidden()
        } else {
            let task_count = job.tasks.len();
            let pb = indicatif::ProgressBar::new(task_count.try_into().unwrap());
            pb.set_style(progress_bar_style(task_count));
            pb.set_prefix(job.name.to_string());
            pb.enable_steady_tick(Duration::from_secs(1));
            pb
        });

        tracing::span!(
            tracing::Level::INFO,
            "Pool::execute",
            name = job.name.as_ref(),
        )
        .in_scope(|| {
            let mut successful = Vec::new();
            let mut failed = Vec::new();
            {
                let state = Mutex::new((&mut task_monitor, &mut successful, &mut failed));
                self.thread_pool().scope(|scope| {
                    let state = &state;
                    for Task { name, task } in job.tasks.drain(..) {
                        scope.spawn(move |_| {
                            tracing::span!(tracing::Level::INFO, "Task::task", name = name.as_ref())
                                .in_scope(|| {
                                    let task_id = {
                                        let mut guard = state.lock().unwrap();
                                        let (task_monitor, _, _) = &mut *guard;
                                        task_monitor.started(name)
                                    };

                                    let result = (task)();

                                    let mut guard = state.lock().unwrap();
                                    let (task_monitor, successful, failed) = &mut *guard;
                                    let name = task_monitor.finished(task_id);
                                    match result {
                                        Ok(ok) => {
                                            successful.push(ExecutionResult { name, result: ok });
                                        }

                                        Err(err) => {
                                            failed.push(ExecutionResult { name, result: err })
                                        }
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
    name: Cow<'task, str>,
    tasks: Vec<Task<'task, T, E>>,
}

impl<'task, T: Send, E: Send> Job<'task, T, E> {
    pub fn with_name(name: impl Into<Cow<'task, str>>) -> Self {
        Job {
            name: name.into(),
            tasks: Vec::new(),
        }
    }

    pub fn add_task<F: FnOnce() -> Result<T, E> + Send + 'task>(
        &mut self,
        name: impl Into<Cow<'task, str>>,
        task: F,
    ) {
        self.tasks.push(Task {
            name: name.into(),
            task: Box::new(task),
        });
    }
}

struct Task<'task, T: Send, E: Send> {
    name: Cow<'task, str>,
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
        let ExecutionResults { successful, failed } = pool.execute(job);

        // `ExecutionResult` isn't PartialEq so can't be used with `assert_eq!`. Convert to a tuple.
        let mut successful = successful
            .iter()
            .map(|ExecutionResult { name, result }| -> (&str, usize) { (&*name, *result) })
            .collect::<Vec<_>>();
        successful.sort();
        let mut failed = failed
            .iter()
            .map(|ExecutionResult { name, result }| -> (&str, &str) { (&*name, &*result) })
            .collect::<Vec<_>>();
        failed.sort();

        assert_eq!(successful.as_slice(), &[("first", 0), ("second", 1)]);
        assert_eq!(failed.as_slice(), &[("third", "3")]);
    }
}
