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

