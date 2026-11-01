// Behavioral tests supplementing the instructor's unchanged starter tests.
use super::*;
use tokio::time::{timeout, Duration};

// Makes an EECS301 request with the prerequisite already completed.
fn request(action: EnrollmentAction) -> EnrollmentTask {
    EnrollmentTask {
        id: "test-task".to_string(),
        action,
        student_id: "S001".to_string(),
        course_id: Course::EECS301,
        prereqs_taken: vec![Course::EECS201],
    }
}

// Sets up a count for testing a specific capacity boundary.
fn counts_at(count: u32) -> EnrollmentCounts {
    Arc::new(Mutex::new([(Course::EECS301, count)].into_iter().collect()))
}

// Register takes the last seat, then rejects another request without incrementing.
#[tokio::test]
async fn register_respects_capacity() {
    let counts = counts_at(39);
    let task = request(EnrollmentAction::Register);
    assert!(process_enrollment_task(&task, counts.clone()).await.is_ok());
    let error = process_enrollment_task(&task, counts.clone())
        .await
        .unwrap_err();
    assert!(error.contains("full"));
    assert_eq!(counts.lock().unwrap()[&Course::EECS301], 40);
}

// Drop reaches zero and never underflows, including for a missing map entry.
#[tokio::test]
async fn drop_stops_at_zero() {
    let counts = counts_at(1);
    let task = request(EnrollmentAction::Drop);
    for _ in 0..3 {
        assert!(process_enrollment_task(&task, counts.clone()).await.is_ok());
    }
    assert_eq!(counts.lock().unwrap()[&Course::EECS301], 0);
    let empty = Arc::new(Mutex::new(HashMap::new()));
    assert!(process_enrollment_task(&task, empty.clone()).await.is_ok());
    assert_eq!(empty.lock().unwrap()[&Course::EECS301], 0);
}

// CanEnroll checks both conditions without reserving a seat.
#[tokio::test]
async fn can_enroll_checks_capacity_and_prerequisites() {
    let counts = counts_at(39);
    let mut task = request(EnrollmentAction::CanEnroll);
    assert!(process_enrollment_task(&task, counts.clone()).await.is_ok());
    assert_eq!(counts.lock().unwrap()[&Course::EECS301], 39);
    task.prereqs_taken.clear();
    assert!(process_enrollment_task(&task, counts.clone())
        .await
        .unwrap_err()
        .contains("missing"));
    task.prereqs_taken.push(Course::EECS201);
    assert!(process_enrollment_task(&task, counts_at(40))
        .await
        .unwrap_err()
        .contains("full"));
    task.prereqs_taken.clear();
    assert!(process_enrollment_task(&task, counts_at(40))
        .await
        .unwrap_err()
        .contains("full"));
}

// The real status operation returns its current count and treats absent entries as zero.
#[tokio::test]
async fn status_reports_counts() {
    let task = request(EnrollmentAction::EnrollmentStatus);
    for count in [0, 20, 40] {
        let message = process_enrollment_task(&task, counts_at(count))
            .await
            .unwrap();
        assert!(message.contains(&format!("{}/40 seats filled", count)));
    }
    let message = process_enrollment_task(&task, Arc::new(Mutex::new(HashMap::new())))
        .await
        .unwrap();
    assert!(message.contains("0/40 seats filled"));
}

// Exercises the prerequisites of every supplied course, including the empty list.
#[tokio::test]
async fn prerequisites_for_all_courses() {
    for course in [
        Course::EECS101,
        Course::EECS102,
        Course::EECS201,
        Course::EECS301,
        Course::EECS303,
    ] {
        let mut task = request(EnrollmentAction::PrerequisiteCheck);
        task.prereqs_taken = course.prerequisites();
        task.course_id = course;
        assert!(process_enrollment_task(&task, counts_at(0)).await.is_ok());
        if !task.prereqs_taken.is_empty() {
            task.prereqs_taken.pop();
            assert!(process_enrollment_task(&task, counts_at(0)).await.is_err());
        }
    }
}

// Starting from empty state, the samples really reach capacity and cover both prerequisite outcomes.
#[tokio::test]
async fn samples_demonstrate_required_scenarios() {
    let counts = initial_enrollment_counts();
    let tasks = create_sample_tasks();
    assert!(tasks.len() >= 5);
    let mut full = false;
    let mut met = false;
    let mut missing = false;
    let mut ids = std::collections::HashSet::new();
    for task in tasks {
        assert!(ids.insert(task.id.clone()));
        let result = process_enrollment_task(&task, counts.clone()).await;
        if let Err(message) = &result {
            full |= message.contains("full");
        }
        if matches!(task.action, EnrollmentAction::PrerequisiteCheck) {
            met |= result.is_ok();
            missing |= result.is_err();
        }
    }
    assert!(full && met && missing);
    assert_eq!(counts.lock().unwrap()[&Course::EECS303], 30);
}

// Competing operations cannot overbook even when only one seat remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_registrations_do_not_overbook() {
    let counts = counts_at(39);
    let mut handles = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(20));
    for _ in 0..20 {
        let counts = counts.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            process_enrollment_task(&request(EnrollmentAction::Register), counts).await
        }));
    }
    let mut successes = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            successes += 1;
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(counts.lock().unwrap()[&Course::EECS301], 40);
}

// Checks per-task multiplicity and final shared capacity through the actual workers.
#[tokio::test]
async fn processor_distributes_tasks_and_shares_counts() {
    let (processor, mut results) = EnrollmentProcessor::new(4);
    let mut tasks = Vec::new();
    for i in 0..50 {
        let mut task = request(EnrollmentAction::Register);
        task.id = format!("register-{}", i);
        tasks.push(task);
    }
    for task in tasks {
        processor.submit_task(task).unwrap();
    }
    // Closing submissions lets both kinds of worker drain their channels and exit.
    drop(processor);
    let completed = timeout(Duration::from_secs(5), async {
        let mut per_task = HashMap::new();
        let mut successes = 0;
        while let Some(result) = results.recv().await {
            *per_task.entry(result.task_id).or_insert(0) += 1;
            successes += usize::from(result.success);
        }
        (per_task, successes)
    })
    .await
    .expect("Workers did not finish");
    let copies = if cfg!(feature = "work-queue") { 1 } else { 4 };
    assert_eq!(completed.0.len(), 50);
    assert!(completed.0.values().all(|count| *count == copies));
    assert_eq!(completed.1, 40);
}

// A processor with no workers returns an error instead of silently accepting work.
#[tokio::test]
async fn submission_without_workers_fails() {
    let (processor, mut results) = EnrollmentProcessor::new(0);
    assert!(processor
        .submit_task(request(EnrollmentAction::Register))
        .is_err());
    assert!(results.recv().await.is_none());
}
