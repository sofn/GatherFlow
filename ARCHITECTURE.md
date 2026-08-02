# GatherFlow Architecture

## Design Constraints

GatherFlow is built on top of Java 25's preview `java.util.stream.Gatherer` API ([JEP 485](https://openjdk.org/jeps/485)). The following constraints shape every gatherer in the project:

- **Bounded, pull-based Java Streams.** All inputs are finite `java.util.stream.Stream` instances. There is no support for unbounded or push-based sources.
- **Element-local timestamps.** Time-based operators (`debounce`, `throttleFirst/Last`, `bufferTime`, `timestamp`, `timeInterval`, `delay`, `sessionWindow`, `tumblingTimeWindow`, `slidingTimeWindow`) accept a `ToLongFunction` timestamp extractor. They are deterministic and testable, but they do **not** use wall-clock time, event time, watermarks, or late-data handling.
- **Sequential-only execution.** Every gatherer is created with `Gatherer.ofSequential(...)`. The operators rely on encounter order and do not support parallel streams.
- **`Gatherer` short-circuit contract.** An integrator returns `boolean`: `true` asks for the next element, `false` signals that no more elements are needed. `downstream.push(...)` also returns `boolean`, which must be propagated to stop upstream work when a downstream operator (e.g. `limit` or `findFirst`) has short-circuited.

## Module Responsibilities

| Module | Package | Responsibility |
|---|---|---|
| `sequence` | `com.lesofn.gatherflow.sequence` | Scala/Vavr-style functional sequence operators: `scanLeft`, `scanRight`, `sliding`, `grouped`, `zip`, `partition`, `groupBy`, etc. |
| `window` | `com.lesofn.gatherflow.window` | Flink-inspired windowing and routing operators: `tumblingWindow`, `slidingWindow`, `sessionWindow`, `globalWindow`, keyed windows, `split`/`selectTag`, `connect`/`coMap`, `union`. |
| `reactive` | `com.lesofn.gatherflow.reactive` | RxJava/Reactor-style timing, side-effect, error-handling, and composition operators: `debounce`, `throttleFirst/Last`, `bufferTime`, `doOnNext/Complete/Error`, `onErrorReturn/Resume`, `retry`, `withLatestFrom`, etc. |

## Public Record Carriers

GatherFlow exposes a small set of immutable record carriers. They are intentionally simple and are produced/consumed by the public gatherer methods.

| Record | Module | Purpose | Immutability Notes |
|---|---|---|---|
| `Window<T>` | `window` | Carries `windowId`, `startIndex`, `endIndex`, and `elements` for each emitted window. | Canonical constructor defensively copies the input list and wraps it with `Collections.unmodifiableList`. |
| `PartitionResult<T>` | `sequence` | Holds the matching and non-matching halves of `partition`. | Built from `List.copyOf` in `SequenceGatherers.partition`. |
| `Tagged<T>` | `window` | Pairs a `String tag` (e.g. `"main"` / `"other"`) with a `value`. Used by `split`/`selectTag` and `connect`/`coMap`. | Standard Java record; fields are immutable references. |
| `KeyedResult<K, R>` | `window` | Pairs a grouping key with the computed window result. | Standard Java record. |
| `Notification<T>` | `reactive` | Sealed interface with `OnNext`, `OnError`, and `OnComplete` records for `materialize`/`dematerialize`. | `OnNext` and `OnError` validate non-null arguments; `OnComplete` is stateless. |
| `Timestamped<T>` | `reactive` | Wraps an element with a `long timestamp`. | Standard Java record. |
| `Timed<T>` | `reactive` | Wraps an element with an elapsed time value. | Standard Java record. |

Records provide shallow immutability. The collections inside `Window` and `PartitionResult` are unmodifiable, but the elements they contain are not deep-copied.

## Note on `downstream.push` Return Propagation

`downstream.push(element)` returns `false` when the downstream pipeline no longer accepts elements. Correct gatherers must:

1. Return that value from their `integrator` so the upstream stops sending more input.
2. Stop emitting in their `finisher` (the optional completion function) once `downstream.push(...)` returns `false`.

Ignoring the return value can cause unnecessary computation or incorrect behavior after short-circuiting operators. This is the dominant source of subtle bugs in `Gatherer` implementations, and all GatherFlow operators propagate the result.

## What is Not Supported

- Parallel streams.
- Event time, watermarks, or out-of-order / late-data handling.
- Checkpoints, savepoints, distributed state, or exactly-once semantics.
- Wall-clock async scheduling (`interval`, `timeout`, `subscribeOn`, `observeOn`, etc.).
