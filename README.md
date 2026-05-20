# GatherFlow

[中文版](./README.zh-CN.md)

**GatherFlow** demonstrates the power of Java 25's `Stream.gather()` API ([JEP 485](https://openjdk.org/jeps/485)) by implementing three families of stream operators modeled after well-known frameworks:

| Module | Inspiration | Description |
|---|---|---|
| `sequence` | Scala Collections / Vavr | Functional sequence operators |
| `window` | Apache Flink | Windowing & keyed stream operators |
| `reactive` | RxJava / Project Reactor | Reactive-style timing & error operators |

---

## Requirements

- Java 25+ (preview features enabled via `--enable-preview`)
- Gradle 8+

---

## Build & Test

```bash
# Run all tests with coverage
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
| `scanLeft` | `(zero, BiFunction)` | Left prefix scan, emits each step including initial | Scala `scanLeft` |
| `scanRight` | `(zero, BiFunction)` | Right suffix scan, buffers then emits reversed steps | Scala `scanRight` |
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
| `cycle` | `(times)` | Repeat stream elements up to N total | Scala / Vavr |
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

> Simulates Flink's stream windowing semantics on bounded Java Streams.
> All window operators emit [`Window<T>`](src/main/java/com/lesofn/gatherflow/window/Window.java)
> records with metadata (windowId, startIndex, endIndex, elements).

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
| `connect(Iterable)` | Merge two-typed streams as `Tagged<Object>` | `DataStream.connect()` |
| `coMap(mapLeft, mapRight)` | Map each side of a connected stream | `ConnectedStreams.map()` |
| `union(Iterable)` | Concatenate another iterable after the stream | `DataStream.union()` |

**Feasibility summary:**

| Flink Concept | Gatherer Support | Notes |
|---|---|---|
| Tumbling / Sliding / Session / Global Window | Full | Count- and time-based |
| KeyBy + Window + Reduce/Aggregate | Full | Per-key windowing |
| ProcessWindowFunction | Full | Full window context |
| Connect / CoMap | Partial | Via tagged union |
| Split / Side Output | Partial | Tag-based routing |
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
| `doOnComplete(Runnable)` | Side-effect at stream end | `doOnComplete` |
| `doOnError(Consumer<Throwable>)` | Observe and re-throw exceptions | `doOnError` |
| `doFinally(Consumer<String>)` | Side-effect on any termination | `doFinally` |

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
| `withLatestFrom(other, combiner)` | Combine with latest from another iterable | `withLatestFrom` |

#### Selection Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `elementAt(index)` | Emit element at index or `Optional.empty()` | `elementAt` |
| `first()` | Emit first element or `Optional.empty()` | `first` / `next` |
| `last()` | Emit last element or `Optional.empty()` | `last` |
| `skipLast(n)` | Skip last N elements | `skipLast` |
| `takeLast(n)` | Take only last N elements | `takeLast` |
| `distinctUntilChanged()` | Suppress consecutive duplicates | `distinctUntilChanged` |
| `distinctUntilChanged(keyExtractor)` | Suppress consecutive duplicates by key | `distinctUntilChanged(keySelector)` |

#### Aggregation Operators

| Operator | Description | RxJava / Reactor |
|---|---|---|
| `scan(seed, accumulator)` | Scan with seed (includes seed in output) | `scan` |
| `reduceWith(seed, accumulator)` | Reduce with seed, emit single result | `reduce` |
| `collectList()` | Collect all elements into one `List` | `collectList` |
| `mapWithIndex(combiner)` | Pair each element with its index | `index` |
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

[MIT](./LICENSE)
