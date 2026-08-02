# GatherFlow

[中文版](./README.zh-CN.md)

> **API Stability: Experimental**
>
> GatherFlow is a proof-of-concept/experimental library exploring Java 25's `Stream.gather()` API. Public APIs may change in any release until a stable version is announced.

**GatherFlow** demonstrates Java 25's `Stream.gather()` API ([JEP 485](https://openjdk.org/jeps/485)) by providing gatherers that simulate a **subset** of Scala/Vavr sequence, Apache Flink windowing, and RxJava/Reactor reactive semantics on **bounded, pull-based Java Streams**. It is not a replacement for Flink, RxJava, or Reactor, and it does not faithfully replicate their runtime behavior.

| Module | Inspiration | Description |
|---|---|---|
| `sequence` | Scala Collections / Vavr | Functional sequence operators |
| `window` | Apache Flink | Windowing & keyed stream operators |
| `reactive` | RxJava / Project Reactor | Reactive-style timing & composition operators |

---

## Project Goals and Non-Goals

**Goals**

- Explore idiomatic uses of the JEP 485 `Gatherer` API.
- Provide deterministic, test-friendly operators for in-memory, bounded streams.
- Offer a unified vocabulary for common sequence/window/reactive-style transformations on `java.util.stream.Stream`.

**Non-Goals**

- Distributed or parallel stream processing.
- Unbounded sources, event time, watermarks, or async scheduling.
- Replacing Flink, Kafka Streams, RxJava, or Project Reactor in production.
- Exactly-once semantics, checkpoints, or distributed state.

> **What this library is NOT**
>
> - It is **not** an event-time stream processor (no watermarks, no event time).
> - It has **no** checkpointing, distributed state, or exactly-once guarantees.
> - It does **not** support unbounded sources or wall-clock scheduling.
> - All gatherers are **sequential-only**; parallel streams are not supported.

---

## Requirements

- Java 25+ (preview features enabled via `--enable-preview`)
- Gradle 8+

---

## Getting Started

All gatherer factories are `public static` methods on `SequenceGatherers`, `WindowGatherers`, and `ReactiveGatherers`. Use `import static ...*` to call them directly inside `Stream.gather(...)`:

```java
import java.util.List;
import java.util.stream.Stream;
import static com.lesofn.gatherflow.sequence.SequenceGatherers.scanLeft;
import static com.lesofn.gatherflow.window.WindowGatherers.tumblingWindow;
import static com.lesofn.gatherflow.window.WindowGatherers.windowReduce;

public class QuickStart {
    public static void main(String[] args) {
        List<Integer> sums = Stream.of(1, 2, 3, 4, 5, 6)
                .gather(tumblingWindow(2))
                .gather(windowReduce(Integer::sum))
                .toList();
        System.out.println(sums); // [3, 7, 11]

        List<Integer> scanned = Stream.of(1, 2, 3, 4, 5)
                .gather(scanLeft(0, Integer::sum))
                .toList();
        System.out.println(scanned); // [0, 1, 3, 6, 10, 15]
    }
}
```

Compile and run with Java 25 preview enabled:

```bash
javac --enable-preview --release 25 -cp gatherflow-*.jar QuickStart.java
java --enable-preview -cp .:gatherflow-*.jar QuickStart
```

In a Gradle project, add this project as a dependency and ensure that compile and exec tasks enable preview:

```groovy
tasks.withType(JavaCompile).configureEach {
    options.compilerArgs += ['--enable-preview']
}
tasks.withType(JavaExec).configureEach {
    jvmArgs += ['--enable-preview']
}
```

---

## Build & Test

```bash
# Run all tests with coverage and verification
./gradlew check

# Run tests only
./gradlew test

# Generate JaCoCo HTML report
./gradlew jacocoTestReport
```

Coverage reports are generated under `build/reports/jacoco/`.

---

## Operator Reference

### `SequenceGatherers` — Scala / Vavr Inspired

| Operator | Signature | Description | Inspired by |
|---|---|---|---|
| `scanLeft` | `(zero, BiFunction<A, T, A>)` | Left prefix scan, emits each step including initial | Scala `scanLeft` |
| `scanRight` | `(zero, BiFunction<T, A, A>)` | Right suffix scan, buffers then emits reversed steps; `BiFunction` receives `(element, accumulator)` | Scala `scanRight` |
| `sliding` | `(size)` / `(size, step)` | Overlapping sliding windows | Scala `sliding` |
| `grouped` | `(size)` | Non-overlapping fixed-size chunks | Scala `grouped` |
| `intersperse` | `(separator)` | Insert separator between consecutive elements | Vavr / Scala |
| `zipWithIndex` | `()` | Pair each element with its zero-based index | Scala `zipWithIndex` |
| `zip` | `(Iterable)` | Pair elements with another iterable, stop at shorter | Scala `zip` |
| `distinctBy` | `(keyExtractor)` | Keep first occurrence by key | Scala `distinctBy` |
| `takeWhile` | `(predicate)` | Take while predicate holds; short-circuits on failure | Scala `takeWhile` |
| `dropWhile` | `(predicate)` | Drop while predicate holds; forward remaining | Scala `dropWhile` |
| `partition` | `(predicate)` | Split into matching / non-matching, emit single result | Scala `partition` |
| `flatMap` | `(Function<T, Iterable<R>>)` | One-to-many element expansion | Scala `flatMap` |
| `collect` | `(predicate, mapper)` | Filter and map simultaneously | Scala `collect` |
| `peek` | `(Consumer)` | Side-effect observation, pass through unchanged | Vavr `peek` |
| `prepend` | `(element)` | Emit element first, then stream | Vavr `prepend` |
| `append` | `(element)` | Emit stream, then element | Vavr `append` |
| `cycle` | `(int times)` | Repeat elements up to `times` total emitted; `[1,2,3].cycle(7) → [1,2,3,1,2,3,1]` | Scala / Vavr |
| `interleave` | `(Iterable)` | Alternate elements from stream and iterable | Vavr `interleave` |
| `reverse` | `()` | Emit elements in reverse order | Scala `reverse` |
| `slice` | `(fromIndex, toIndex)` | Emit elements in index range [from, to) | Scala `slice` |
| `foldLeft` | `(zero, BiFunction)` | Fold to single result emitted at stream end | Scala `foldLeft` |
| `reduceLeft` | `(BinaryOperator)` | Reduce to `Optional` result | Scala `reduceLeft` |
| `groupBy` | `(keyExtractor)` | Group into `Map<K, List<T>>`, emitted at end | Scala `groupBy` |
| `unfold` | `(seed, Function)` | Generate elements by unfolding a state | Scala `unfold` |

**Example — scanLeft:**
```java
Stream.of(1, 2, 3, 4, 5)
      .gather(scanLeft(0, Integer::sum))
      .toList();
// [0, 1, 3, 6, 10, 15]
```

**Example — partition:**
```java
PartitionResult<Integer> p = Stream.of(1, 2, 3, 4, 5)
      .gather(partition(x -> x % 2 == 0))
      .findFirst().orElseThrow();
// p.matching()    -> [2, 4]
// p.nonMatching() -> [1, 3, 5]
```

---

### `WindowGatherers` — Apache Flink Inspired

> Simulates a subset of Flink's windowing semantics on bounded Java Streams.
> All window operators emit [`Window<T>`](src/main/java/com/lesofn/gatherflow/window/Window.java)
> records with metadata (windowId, startIndex, endIndex, elements).
> Time-based windows use element-local timestamps; there is no event time or watermarks.

#### Window Operators

| Operator | Description | Flink Equivalent |
|---|---|---|
| `tumblingWindow(size)` | Non-overlapping count windows | `countWindow(size)` |
| `slidingWindow(size)` / `slidingWindow(size, slide)` | Overlapping count windows | `countWindow(size, slide)` |
| `sessionWindow(gap, tsExtractor)` | Gap-based session windows | `EventTimeSessionWindows.withGap` |
| `globalWindow()` | Single window containing all elements | `GlobalWindows.create()` |
| `tumblingTimeWindow(size, tsExtractor)` | Non-overlapping time windows | Tumbling event-time window |
| `slidingTimeWindow(size, slide, tsExtractor)` | Overlapping time windows | Sliding event-time window |

#### Window Result Operators

| Operator | Description | Flink Equivalent |
|---|---|---|
| `windowReduce(BinaryOperator)` | Reduce window elements to a single value | `WindowedStream.reduce()` |
| `windowAggregate(createAcc, add, getResult)` | Aggregate with separate accumulator type | `WindowedStream.aggregate()` |
| `windowProcess(Function<Window, Iterable>)` | Full window access with multiple outputs | `WindowedStream.process()` |
| `windowApply(Function<Window<T>, R>)` | Transform each `Window<T>` into a single result | `AllWindowedStream.apply()` |
| `windowCount()` | Emit the element count of each window | `WindowedStream.count()` |
| `windowMin(Comparator)` | Minimum element of each window | — |
| `windowMax(Comparator)` | Maximum element of each window | — |
| `windowSum(ToDoubleFunction)` | Sum of each window | — |

#### Keyed Stream Operators

| Operator | Description | Flink Equivalent |
|---|---|---|
| `keyBy(keyExtractor)` | Tag each element with its key | `DataStream.keyBy()` |
| `keyedTumblingWindow(keyExtractor, size)` | Tumbling windows per key | `keyBy().countWindow(n)` |
| `keyedWindowReduce(BinaryOperator)` | Reduce within each keyed window | `KeyedStream.reduce()` |
| `keyedWindowAggregate(...)` | Aggregate within each keyed window | `KeyedStream.aggregate()` |

#### Routing Operators

| Operator | Description | Flink Equivalent |
|---|---|---|
| `split(classifier)` | Tag elements by string label | `DataStream.split()` |
| `selectTag(tag)` | Filter `Tagged<T>` stream by tag | `SplitStream.select()` |
| `connect(Iterable)` | Interleave main stream and another iterable as `Tagged<T>` with tags `"main"` and `"other"`; both sides are same-typed | `DataStream.connect()` |
| `coMap(mapMain, mapOther)` | Map each side of a `Tagged<T>` stream by tag | `ConnectedStreams.map()` |
| `union(Iterable)` | Concatenate another iterable after the stream | `DataStream.union()` |

**Feasibility summary:**

| Flink Concept | Gatherer Support | Notes |
|---|---|---|
| Tumbling / Sliding / Session windows | Partial | Bounded streams, element-local timestamps, no triggers |
| Global Window | Partial | Bounded; emits one window at stream end, no custom triggers |
| KeyBy + Window + Reduce/Aggregate | Full | Per-key windowing on bounded streams |
| ProcessWindowFunction | Full | Full window context |
| Connect / CoMap | Partial | Via tagged union of same-typed values |
| Split / Side Output | Partial | Tag-based routing |
| Union | Partial | Concatenates another iterable; not a true parallel union |
| Event Time / Watermarks | Not feasible | Requires unbounded push model |
| Checkpointing / Exactly-once | Not applicable | Single JVM, no distributed state |

**Example — tumbling window + reduce:**
```java
Stream.of(1, 2, 3, 4, 5, 6)
      .gather(tumblingWindow(2))
      .gather(windowReduce(Integer::sum))
      .toList();
// [3, 7, 11]
```

**Example — keyed tumbling window:**
```java
record Event(String key, int value) {}
Stream.of(new Event("A",1), new Event("B",10),
          new Event("A",2), new Event("B",20))
      .gather(keyedTumblingWindow(Event::key, 2))
      .gather(keyedWindowReduce((e1, e2) -> new Event(e1.key(), e1.value() + e2.value())))
      .toList();
// [KeyedResult(A, Event(A,3)), KeyedResult(B, Event(B,30))]
```

---

### `ReactiveGatherers` — RxJava / Project Reactor Inspired

> Only operators not already in Java `Stream` are implemented here.
> Time-based operators use element-embedded timestamps (deterministic) rather than wall-clock time.
> These gatherers simulate a subset of RxJava/Reactor semantics on bounded, pull-based streams.

#### Timing Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `debounce(timeout, tsExtractor)` | Emit last element when gap exceeds timeout | `debounce` |
| `throttleFirst(windowSize, tsExtractor)` | Emit first element per time window | `throttleFirst` / `sampleFirst` |
| `throttleLast(windowSize, tsExtractor)` | Emit last element per time window | `throttleLast` / `sample` |
| `bufferTime(timespan, tsExtractor)` | Batch elements into time-bucketed lists | `buffer(Duration)` |
| `timestamp(tsExtractor)` | Wrap each element with its timestamp | `timestamp` |
| `timeInterval(tsExtractor)` | Measure elapsed time between elements | `timeInterval` / `elapsed` |
| `delay(tsExtractor)` | Re-order elements by timestamp | `delayElements` (sort-based) |

#### Side-Effect Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `doOnNext(Consumer)` | Side-effect per element, pass through | `doOnNext` |
| `doOnComplete(Runnable)` | Side-effect at normal stream end | `doOnComplete` |
| `doOnError(Consumer<Throwable>)` | Observe `RuntimeException` / `Error` thrown while pushing an element downstream, then re-throw; does not catch upstream map/filter errors | `doOnError` |
| `doFinally(Consumer<String>)` | Side-effect only on normal completion, receiving the string `"complete"`; cannot observe errors because Java Streams have no error channel | `doFinally` |

#### Error Handling Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `onErrorReturn(mapper, fallback)` | Fallback value when mapper throws | `onErrorReturn` |
| `onErrorResume(mapper, fallbackFactory)` | Fallback iterable when mapper throws | `onErrorResume` |
| `retry(mapper, maxRetries)` | Retry mapper on failure | `retry` |

#### Stream Composition Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `repeat(times)` | Repeat buffered elements N times | `repeat` |
| `defaultIfEmpty(value)` | Emit default when stream is empty | `defaultIfEmpty` |
| `switchIfEmpty(Iterable)` | Switch to fallback when stream is empty | `switchIfEmpty` |
| `startWith(Iterable)` | Prepend iterable before stream | `startWith` |
| `concatWith(Iterable)` | Append iterable after stream | `concatWith` |
| `withLatestFrom(other, combiner)` | For each main element, consume one value from `other` and combine it with the latest `other` value seen so far; main elements received before `other` has produced any value are skipped; if `other` is shorter than the main stream, the last `other` value is reused for the remaining main elements | `withLatestFrom` |

#### Selection Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `elementAt(index)` | Emit element at index or `Optional.empty()` | `elementAt` |
| `first()` | Emit first element or `Optional.empty()` | `first` / `next` |
| `last()` | Emit last element or `Optional.empty()` | `last` |
| `skipLast(n)` | Skip last N elements | `skipLast` |
| `takeLast(n)` | Take only last N elements | `takeLast` |
| `distinctUntilChanged()` | Suppress consecutive duplicates | `distinctUntilChanged` |
| `distinctUntilChanged(keyExtractor)` | Suppress consecutive duplicates by key | `distinctUntilChanged` |

#### Aggregation Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `scan(seed, accumulator)` | Scan with seed (includes seed in output) | `scan` |
| `reduceWith(seed, accumulator)` | Reduce with seed, emit single result | `reduce` |
| `collectList()` | Collect all elements into one `List` | `collectList` |
| `mapWithIndex(combiner)` | Pair each element with its index; the `BiFunction` receives `(Long index, T element)` | `index` |
| `materialize()` | Wrap elements as `Notification<T>` | `materialize` |
| `dematerialize()` | Unwrap `Notification<T>` stream | `dematerialize` |

**Example — debounce:**
```java
record Event(long ts, String v) {}
Stream.of(new Event(0,"a"), new Event(30,"b"), new Event(100,"c"))
      .gather(debounce(50, Event::ts))
      .toList();
// [Event(30,"b"), Event(100,"c")]
// "b" emitted because gap(100-30=70) > 50; "c" emitted at stream end
```

**Example — distinctUntilChanged:**
```java
Stream.of(1, 1, 2, 2, 3, 1, 1)
      .gather(distinctUntilChanged())
      .toList();
// [1, 2, 3, 1]
```

---

## Project Structure

```
src/
  main/java/com/lesofn/gatherflow/
    sequence/
      SequenceGatherers.java      # Scala/Vavr operators
      PartitionResult.java
    window/
      WindowGatherers.java        # Flink-inspired operators
      Window.java
      KeyedResult.java
      Tagged.java
    reactive/
      ReactiveGatherers.java      # RxJava/Reactor operators
      Notification.java
      Timed.java
      Timestamped.java
  test/java/com/lesofn/gatherflow/
    sequence/   SequenceGatherersTest.java
    window/     WindowGatherersTest.java
    reactive/   ReactiveGatherersTest.java
    StreamingOperatorTest.java   # virtual-thread streaming tests
```

---

## License

[Apache-2.0](./LICENSE)
