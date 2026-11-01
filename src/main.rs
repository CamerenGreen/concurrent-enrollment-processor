// EECS 468, Project 1: Concurrent Enrollment Processor.
// Processes registration, drop, prerequisite, and status requests using either
// broadcast subscriptions or a shared crossbeam work queue.

#[cfg(feature = "work-queue")]
use crossbeam_channel::{unbounded, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
#[cfg(not(feature = "work-queue"))]
use tokio::sync::broadcast;
use tokio::sync::mpsc;

// All workers share the same course counts, protected by one mutex.
type EnrollmentCounts = Arc<Mutex<HashMap<Course, u32>>>;

// One completed task, including either its success message or its error.
struct TaskResult {
    task_id: String,
    success: bool,
    result_message: String,
}

// The courses supplied in the assignment template.
#[derive(Clone, Debug, PartialEq, Hash, Eq)]
enum Course {
    EECS101,
    EECS102,
    EECS201,
    EECS301,
    EECS303,
}

impl Course {
    // Lists the completed courses required before taking this course.
    fn prerequisites(&self) -> Vec<Course> {
        match self {
            Course::EECS101 => vec![],
            Course::EECS102 => vec![Course::EECS101],
            Course::EECS201 => vec![Course::EECS101, Course::EECS102],
            Course::EECS301 => vec![Course::EECS201],
            Course::EECS303 => vec![Course::EECS201],
        }
    }

    // Returns the enrollment limit supplied in the template.
    fn max_seats(&self) -> u32 {
        match self {
            Course::EECS101 => 100,
            Course::EECS102 => 80,
            Course::EECS201 => 60,
            Course::EECS301 => 40,
            Course::EECS303 => 30,
        }
    }
}

// One student's requested operation and their completed courses.
#[derive(Clone, Debug)]
struct EnrollmentTask {
    id: String,
    action: EnrollmentAction,
    student_id: String,
    course_id: Course,
    prereqs_taken: Vec<Course>,
}

// The five enrollment operations required by the assignment.
#[derive(Clone, Debug)]
enum EnrollmentAction {
    Register,
    Drop,
    PrerequisiteCheck,
    EnrollmentStatus,
    CanEnroll,
}

// Checks every prerequisite without needing the shared enrollment lock.
fn check_prerequisites(task: &EnrollmentTask) -> Result<String, String> {
    for course in task.course_id.prerequisites() {
        if !task.prereqs_taken.contains(&course) {
            return Err(format!(
                "Student {} is missing {:?}, required for {:?}",
                task.student_id, course, task.course_id
            ));
        }
    }
    Ok(format!(
        "Student {} meets all prerequisites for {:?}",
        task.student_id, task.course_id
    ))
}

// Performs one operation. Checking capacity and changing the count happen
// under the same lock, so two workers cannot claim the same last seat.
async fn process_enrollment_task(
    task: &EnrollmentTask,
    enrollment_counts: EnrollmentCounts,
) -> Result<String, String> {
    match task.action {
        EnrollmentAction::Register => {
            let mut counts = enrollment_counts
                .lock()
                .map_err(|_| "Enrollment count lock was poisoned".to_string())?;
            let count = counts.entry(task.course_id.clone()).or_insert(0);
            if *count >= task.course_id.max_seats() {
                return Err(format!("{:?} is full", task.course_id));
            }
            *count += 1;
            Ok(format!(
                "Student {} registered for {:?}",
                task.student_id, task.course_id
            ))
        }
        EnrollmentAction::Drop => {
            let mut counts = enrollment_counts
                .lock()
                .map_err(|_| "Enrollment count lock was poisoned".to_string())?;
            let count = counts.entry(task.course_id.clone()).or_insert(0);
            if *count > 0 {
                *count -= 1;
            }
            Ok(format!(
                "Student {} dropped {:?}",
                task.student_id, task.course_id
            ))
        }
        EnrollmentAction::PrerequisiteCheck => check_prerequisites(task),
        EnrollmentAction::EnrollmentStatus => {
            let count = {
                let counts = enrollment_counts
                    .lock()
                    .map_err(|_| "Enrollment count lock was poisoned".to_string())?;
                counts.get(&task.course_id).copied().unwrap_or(0)
            };
            Ok(format!(
                "{:?}: {}/{} seats filled",
                task.course_id,
                count,
                task.course_id.max_seats()
            ))
        }
        EnrollmentAction::CanEnroll => {
            let count = {
                let counts = enrollment_counts
                    .lock()
                    .map_err(|_| "Enrollment count lock was poisoned".to_string())?;
                counts.get(&task.course_id).copied().unwrap_or(0)
            };
            if count >= task.course_id.max_seats() {
                return Err(format!("{:?} is full", task.course_id));
            }
            check_prerequisites(task)?;
            // This is a check only; Register is responsible for claiming a seat.
            Ok(format!(
                "Student {} can enroll in {:?}",
                task.student_id, task.course_id
            ))
        }
    }
}

// Starts every course at zero, as in the supplied template.
fn initial_enrollment_counts() -> EnrollmentCounts {
    let counts = [
        Course::EECS101,
        Course::EECS102,
        Course::EECS201,
        Course::EECS301,
        Course::EECS303,
    ]
    .into_iter()
    .map(|course| (course, 0))
    .collect();
    Arc::new(Mutex::new(counts))
}

// Converts an operation's Result into the message expected by main().
fn task_result(task: EnrollmentTask, result: Result<String, String>) -> TaskResult {
    let (success, result_message) = match result {
        Ok(message) => (true, message),
        Err(message) => (false, message),
    };
    TaskResult {
        task_id: task.id,
        success,
        result_message,
    }
}

// Supplies 38 requests covering all actions, multiple students and courses,
// full capacity, satisfied prerequisites, and missing prerequisites.
fn create_sample_tasks() -> Vec<EnrollmentTask> {
    let mut tasks = Vec::new();
    // EECS303 starts empty. 31 registrations for 30 seats guarantee failures
    // in either channel pattern without changing the initial course counts.
    for i in 1..=31 {
        tasks.push(EnrollmentTask {
            id: format!("register-303-{}", i),
            action: EnrollmentAction::Register,
            student_id: format!("S{:03}", i),
            course_id: Course::EECS303,
            prereqs_taken: vec![Course::EECS201],
        });
    }
    tasks.extend(vec![
        EnrollmentTask {
            id: "drop-empty".to_string(),
            action: EnrollmentAction::Drop,
            student_id: "S032".to_string(),
            course_id: Course::EECS101,
            prereqs_taken: vec![],
        },
        EnrollmentTask {
            id: "register-102".to_string(),
            action: EnrollmentAction::Register,
            student_id: "S033".to_string(),
            course_id: Course::EECS102,
            prereqs_taken: vec![Course::EECS101],
        },
        EnrollmentTask {
            id: "prereqs-met".to_string(),
            action: EnrollmentAction::PrerequisiteCheck,
            student_id: "S034".to_string(),
            course_id: Course::EECS201,
            prereqs_taken: vec![Course::EECS101, Course::EECS102],
        },
        EnrollmentTask {
            id: "prereqs-missing".to_string(),
            action: EnrollmentAction::PrerequisiteCheck,
            student_id: "S035".to_string(),
            course_id: Course::EECS201,
            prereqs_taken: vec![Course::EECS101],
        },
        EnrollmentTask {
            id: "can-enroll".to_string(),
            action: EnrollmentAction::CanEnroll,
            student_id: "S036".to_string(),
            course_id: Course::EECS301,
            prereqs_taken: vec![Course::EECS201],
        },
        EnrollmentTask {
            id: "cannot-enroll".to_string(),
            action: EnrollmentAction::CanEnroll,
            student_id: "S037".to_string(),
            course_id: Course::EECS301,
            prereqs_taken: vec![],
        },
        EnrollmentTask {
            id: "status-303".to_string(),
            action: EnrollmentAction::EnrollmentStatus,
            student_id: "S038".to_string(),
            course_id: Course::EECS303,
            prereqs_taken: vec![],
        },
    ]);
    tasks
}

// Broadcast delivers a copy of each request to every subscribed worker.
// Work-queue takes precedence if both features are enabled; no features uses
// broadcast too, keeping cargo's feature combinations usable.
#[cfg(not(feature = "work-queue"))]
struct EnrollmentProcessor {
    tx: broadcast::Sender<EnrollmentTask>,
}

#[cfg(not(feature = "work-queue"))]
impl EnrollmentProcessor {
    // Creates the channels, shared counts, and one subscription per worker.
    fn new(num_workers: usize) -> (Self, mpsc::UnboundedReceiver<TaskResult>) {
        let (tx, _rx) = broadcast::channel::<EnrollmentTask>(100);
        let (result_tx, result_rx) = mpsc::unbounded_channel::<TaskResult>();
        let enrollment_counts = initial_enrollment_counts();
        for _ in 0..num_workers {
            let mut task_rx = tx.subscribe();
            let result_tx = result_tx.clone();
            let enrollment_counts = enrollment_counts.clone();
            tokio::spawn(async move {
                loop {
                    match task_rx.recv().await {
                        Ok(task) => {
                            let result =
                                process_enrollment_task(&task, enrollment_counts.clone()).await;
                            if result_tx.send(task_result(task, result)).is_err() {
                                break; // The results listener has closed.
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // Broadcast has a bounded buffer, so report lost requests.
                            eprintln!("Broadcast worker lagged; skipped {} tasks", skipped);
                        }
                    }
                }
            });
        }
        (EnrollmentProcessor { tx }, result_rx)
    }

    // Sends to every active subscriber, reporting a closed channel as an error.
    fn submit_task(&self, task: EnrollmentTask) -> Result<(), String> {
        self.tx
            .send(task)
            .map(|_| ())
            .map_err(|e| format!("Failed to submit task: {}", e))
    }
}

// Cloned crossbeam receivers compete for requests; each request is received once.
#[cfg(feature = "work-queue")]
struct EnrollmentProcessor {
    tx: Sender<EnrollmentTask>,
}

#[cfg(feature = "work-queue")]
impl EnrollmentProcessor {
    // Creates a shared queue and workers with access to the existing runtime.
    fn new(num_workers: usize) -> (Self, mpsc::UnboundedReceiver<TaskResult>) {
        let (tx, task_rx) = unbounded::<EnrollmentTask>();
        let (result_tx, result_rx) = mpsc::unbounded_channel::<TaskResult>();
        let enrollment_counts = initial_enrollment_counts();
        let runtime = tokio::runtime::Handle::current();
        for _ in 0..num_workers {
            let task_rx = task_rx.clone();
            let result_tx = result_tx.clone();
            let enrollment_counts = enrollment_counts.clone();
            let runtime = runtime.clone();
            // Crossbeam recv blocks, so give it an OS thread instead of blocking
            // Tokio's async workers. block_on runs our supplied async function.
            std::thread::spawn(move || {
                while let Ok(task) = task_rx.recv() {
                    let result =
                        runtime.block_on(process_enrollment_task(&task, enrollment_counts.clone()));
                    if result_tx.send(task_result(task, result)).is_err() {
                        break;
                    }
                }
            });
        }
        (EnrollmentProcessor { tx }, result_rx)
    }

    // Moves one request into the queue; a disconnected queue returns an error.
    fn submit_task(&self, task: EnrollmentTask) -> Result<(), String> {
        self.tx
            .send(task)
            .map_err(|e| format!("Failed to submit task: {}", e))
    }
}

