# Concurrent Enrollment Processor

EECS 468 Project 1. A small Rust program that processes university enrollment
requests using either broadcast subscriptions or a crossbeam work queue.

## Run

With Rust and Cargo installed, open a terminal in this folder.

```powershell
# Default: broadcast (each task is processed by all four workers).
cargo run

# Work queue (each task is processed by exactly one worker).
cargo run --no-default-features --features work-queue
```

The provided `main()` is unchanged, including its three-second wait and output
format. Results print as workers finish, so output order varies.

## Test

```powershell
cargo test
cargo test --no-default-features --features work-queue
```

`src/provided_tests.rs` is an unchanged copy of the instructor's tests.
`src/additional_tests.rs` checks actual operations, capacity boundaries,
prerequisites, empty drops, concurrent registration, result multiplicity, and
submission without workers.

Validation on October 29, 2026: all 31 tests passed under each channel mode,
with no compiler warnings. Both sample runs were also checked: 152 broadcast
results, 38 work-queue results, and 30 successful EECS303 registrations per run.

Rust was not installed on this computer. Validation used an isolated toolchain
in the Windows temporary folder, without changing the system PATH. While that
temporary folder exists, you can enable it for the current PowerShell session:

```powershell
$env:RUSTUP_HOME = "$env:TEMP\eecs468-rust-tools\rustup"
$env:CARGO_HOME = "$env:TEMP\eecs468-rust-tools\cargo"
$env:PATH = "$env:CARGO_HOME\bin;$env:PATH"
```

Then use the Cargo commands above. A normal Rust installation also works.

## How it works

- `EnrollmentTask` contains the five required fields. `EnrollmentAction` defines
  the five required operations.
- `process_enrollment_task()` matches the action and returns `Result<String, String>`.
- Counts are shared through `Arc<Mutex<HashMap<Course, u32>>>`. Registration holds
  one lock through both the capacity check and increment to prevent overbooking.
- Register checks capacity and claims a seat. Drop decrements only above zero.
  These follow the assignment's count-based model; individual student rosters
  and duplicate-registration detection are outside its scope.
- PrerequisiteCheck checks every required course. CanEnroll checks capacity first,
  then prerequisites, and does not reserve a seat. EnrollmentStatus reports counts.
- Broadcast workers each subscribe to the same Tokio channel. A request is
  intentionally processed once by each worker, including its enrollment changes.
- Work-queue workers each hold a cloned crossbeam receiver. One worker receives
  each task. Blocking receives run on OS threads; a Tokio runtime handle runs
  the common async processing function on those threads.
- Both patterns send `TaskResult` values through the same Tokio mpsc result channel.

## Sample tasks

There are 38 sample requests. Thirty-one register requests target EECS303's 30
seats, demonstrating full capacity without changing the template's zero starting
counts. The other seven demonstrate an empty drop, another registration, both
prerequisite outcomes, both CanEnroll outcomes, and a status request.

With four workers, broadcast returns 152 results and work queue returns 38.
Both permit only 30 successful EECS303 registrations. Scheduling determines
which student gets the last seat and when the status request observes the count.

The broadcast buffer retains 100 requests, as in the template. If a worker falls
behind by more than that, it reports how many requests it missed to stderr and
continues. The sample workload fits inside that buffer.

## Features

Broadcast is the default. Enabling `work-queue` selects the work queue, including
when both features are enabled. Disabling all features falls back to broadcast.

## References used

The language references were limited to these course notes and the Exam 1 review:

- [September 3: Rust basics](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+3+-+Rust+-+Basics)
- [September 8: Ownership](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+8+-+Rust+-+Ownership)
- [September 10: Borrowing](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+10+-+Rust+-+Borrowing)
- [September 15: Lifetimes and type system](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+15+-+Rust+-+Lifetimes+%26+Type+System)
- [September 17: Type system and error handling](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+17+-+Rust+-+Type+System+Cont.+%26+Error+Handling)
- [September 22: Error handling and concurrency](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+22+-+Rust+-+Error+Handling+Cont.+%26+Concurrency)
- [September 24: Channels and async/await](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+24+-+Rust+-+Channels+Cont.+%2B+Async+%26+Await)
- [September 29: Exam 1 review](https://publish.obsidian.md/fa26-468/lectures/fa26/class+notes/published/Sep+29+-+Exam+1+Review)

The supplied assignment description, template, tests, and grading rubric define
the project requirements. No other Rust tutorials or reference documentation
were used.
