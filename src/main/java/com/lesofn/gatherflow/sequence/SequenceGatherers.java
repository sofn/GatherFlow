package com.lesofn.gatherflow.sequence;

import java.util.*;
import java.util.function.*;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

/**
 * Stream Gatherer operators inspired by Scala Collections API and Vavr.
 *
 * <p>Provides a rich set of intermediate stream operations built on top of
 * Java 25's {@link Gatherer} API (JEP 485), simulating functional collection
 * operators from Scala and Vavr.</p>
 *
 * <h3>Scala-inspired operators:</h3>
 * <ul>
 *   <li>{@link #scanLeft} / {@link #scanRight} — prefix/suffix scans</li>
 *   <li>{@link #sliding} — sliding window (overlapping)</li>
 *   <li>{@link #grouped} — non-overlapping fixed-size groups</li>
 *   <li>{@link #intersperse} — insert element between consecutive items</li>
 *   <li>{@link #zipWithIndex} — pair each element with its index</li>
 *   <li>{@link #distinctBy} — distinct by key selector</li>
 *   <li>{@link #takeWhile} / {@link #dropWhile} — conditional take/drop</li>
 *   <li>{@link #partition} — split stream by predicate into two groups</li>
 *   <li>{@link #flatMap} — one-to-many transformation</li>
 *   <li>{@link #collect} — fold into a mutable container</li>
 * </ul>
 *
 * <h3>Vavr-inspired operators:</h3>
 * <ul>
 *   <li>{@link #peek} — side-effect observation without consuming</li>
 *   <li>{@link #intersperse} — insert element between consecutive items</li>
 *   <li>{@link #prepend} / {@link #append} — add single element before/after</li>
 *   <li>{@link #cycle} — repeat stream elements infinitely (capped)</li>
 *   <li>{@link #interleave} — interleave elements from another iterable</li>
 * </ul>
 */
public final class SequenceGatherers {

    private SequenceGatherers() {}

    // ─────────────────────────────────────────────
    //  Scan operators (Scala scanLeft / scanRight)
    // ─────────────────────────────────────────────

    /**
     * Produces a cumulative scan from the left, emitting each intermediate result.
     *
     * <p>Scala: {@code list.scanLeft(z)(op)}</p>
     * <p>Example: {@code [1,2,3].scanLeft(0, +) → [0, 1, 3, 6]}</p>
     *
     * @param zero  initial accumulator value
     * @param op    binary operator applied to accumulator and element
     * @return a Gatherer that emits each scan step including the initial value
     */
    public static <T, A> Gatherer<T, ?, A> scanLeft(A zero, BiFunction<A, T, A> op) {
        Objects.requireNonNull(op);
        return Gatherer.ofSequential(
                () -> new Object() { A acc = zero; boolean emittedInitial = false; },
                (state, element, downstream) -> {
                    if (!state.emittedInitial) {
                        if (!downstream.push(state.acc)) return false;
                        state.emittedInitial = true;
                    }
                    state.acc = op.apply(state.acc, element);
                    return downstream.push(state.acc);
                },
                (state, downstream) -> {
                    if (!state.emittedInitial) {
                        if (!downstream.push(state.acc)) return;
                    }
                }
        );
    }

    /**
     * Produces a cumulative scan from the right.
     * Since Gatherer processes left-to-right, this buffers all elements first,
     * then applies the operator from right-to-left.
     *
     * <p>Scala: {@code list.scanRight(z)(op)}</p>
     * <p>Example: {@code [1,2,3].scanRight(0, +) → [6, 5, 3, 0]}</p>
     *
     * @param zero  initial accumulator (applied from right)
     * @param op    binary operator: (element, accumulator) → new accumulator
     * @return a Gatherer that emits each right-scan step
     */
    public static <T, A> Gatherer<T, ?, A> scanRight(A zero, BiFunction<T, A, A> op) {
        Objects.requireNonNull(op);
        return Gatherer.ofSequential(
                () -> new Object() { final List<T> buffer = new ArrayList<>(); },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    return true;
                },
                (state, downstream) -> {
                    A acc = zero;
                    List<A> results = new ArrayList<>();
                    results.add(acc);
                    for (int i = state.buffer.size() - 1; i >= 0; i--) {
                        acc = op.apply(state.buffer.get(i), acc);
                        results.add(acc);
                    }
                    Collections.reverse(results);
                    for (A r : results) {
                        if (!downstream.push(r)) return;
                    }
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Sliding / Grouped (Scala sliding, grouped)
    // ─────────────────────────────────────────────

    /**
     * Sliding window of fixed size with step 1 (overlapping).
     *
     * <p>Scala: {@code list.sliding(n)}</p>
     * <p>Example: {@code [1,2,3,4,5].sliding(3) → [[1,2,3],[2,3,4],[3,4,5]]}</p>
     *
     * @param size  window size, must be >= 1
     * @return a Gatherer emitting overlapping windows as Lists
     */
    public static <T> Gatherer<T, ?, List<T>> sliding(int size) {
        return sliding(size, 1);
    }

    /**
     * Sliding window with configurable step.
     *
     * <p>Scala: {@code list.sliding(size, step)}</p>
     * <p>Example: {@code [1,2,3,4,5].sliding(3,2) → [[1,2,3],[3,4,5]]}</p>
     *
     * @param size  window size, must be >= 1
     * @param step  step between window starts, must be >= 1
     * @return a Gatherer emitting sliding windows as Lists
     */
    public static <T> Gatherer<T, ?, List<T>> sliding(int size, int step) {
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        if (step < 1) throw new IllegalArgumentException("step must be >= 1, got " + step);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final ArrayDeque<T> window = new ArrayDeque<>();
                    int count = 0;
                },
                (state, element, downstream) -> {
                    state.window.addLast(element);
                    state.count++;
                    if (state.window.size() > size) {
                        state.window.pollFirst();
                    }
                    if (state.count >= size && (state.count - size) % step == 0) {
                        return downstream.push(new ArrayList<>(state.window));
                    }
                    return true;
                },
                (state, downstream) -> {
                    if (state.count < size) {
                        // Stream ended before a full window was ready.
                        if (!state.window.isEmpty()) {
                            if (!downstream.push(new ArrayList<>(state.window))) return;
                        }
                    } else if ((state.count - size) % step != 0) {
                        // Trailing partial window after the last aligned full window.
                        int lastFullStart = ((state.count - size) / step) * step;
                        int nextStart = lastFullStart + step;
                        int partialSize = state.count - nextStart;
                        if (partialSize > 0) {
                            List<T> current = new ArrayList<>(state.window);
                            if (!downstream.push(new ArrayList<>(
                                    current.subList(current.size() - partialSize, current.size())))) return;
                        }
                    }
                }
        );
    }

    /**
     * Non-overlapping fixed-size groups (chunks).
     *
     * <p>Scala: {@code list.grouped(n)}</p>
     * <p>Example: {@code [1,2,3,4,5].grouped(2) → [[1,2],[3,4],[5]]}</p>
     *
     * @param size  group size, must be >= 1
     * @return a Gatherer emitting non-overlapping groups as Lists
     */
    public static <T> Gatherer<T, ?, List<T>> grouped(int size) {
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        return Gatherer.ofSequential(
                () -> new Object() { final List<T> chunk = new ArrayList<>(); },
                (state, element, downstream) -> {
                    state.chunk.add(element);
                    if (state.chunk.size() == size) {
                        if (!downstream.push(new ArrayList<>(state.chunk))) return false;
                        state.chunk.clear();
                    }
                    return true;
                },
                (state, downstream) -> {
                    if (!state.chunk.isEmpty()) {
                        if (!downstream.push(new ArrayList<>(state.chunk))) return;
                    }
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Intersperse (Vavr intersperse / Scala mkString-like)
    // ─────────────────────────────────────────────

    /**
     * Insert a separator element between every two consecutive elements.
     *
     * <p>Vavr: {@code list.intersperse(sep)}</p>
     * <p>Example: {@code [1,2,3].intersperse(0) → [1,0,2,0,3]}</p>
     *
     * @param separator  element to insert between items
     * @return a Gatherer that intersperses the separator
     */
    public static <T> Gatherer<T, ?, T> intersperse(T separator) {
        return Gatherer.ofSequential(
                () -> new Object() { boolean first = true; },
                (state, element, downstream) -> {
                    if (!state.first) {
                        if (!downstream.push(separator)) return false;
                    }
                    state.first = false;
                    return downstream.push(element);
                }
        );
    }

    // ─────────────────────────────────────────────
    //  ZipWithIndex (Scala zipWithIndex)
    // ─────────────────────────────────────────────

    /**
     * Pair each element with its zero-based index.
     *
     * <p>Scala: {@code list.zipWithIndex}</p>
     * <p>Example: {@code ["a","b","c"].zipWithIndex → [("a",0),("b",1),("c",2)]}</p>
     *
     * @return a Gatherer emitting {@link Map.Entry} of element and index
     */
    public static <T> Gatherer<T, ?, Map.Entry<T, Long>> zipWithIndex() {
        return Gatherer.ofSequential(
                () -> new Object() { long index = 0; },
                (state, element, downstream) -> {
                    long idx = state.index++;
                    return downstream.push(new AbstractMap.SimpleImmutableEntry<>(element, idx));
                }
        );
    }

    // ─────────────────────────────────────────────
    //  DistinctBy (Scala distinctBy)
    // ─────────────────────────────────────────────

    /**
     * Keep only the first occurrence of elements as determined by a key selector.
     *
     * <p>Scala: {@code list.distinctBy(keyExtractor)}</p>
     * <p>Example: {@code ["aa","bb","ab"].distinctBy(String::length) → ["aa","ab"]}</p>
     *
     * @param keyExtractor  function to compute the distinctness key
     * @return a Gatherer that emits only first-occurrence elements per key
     */
    public static <T, K> Gatherer<T, ?, T> distinctBy(Function<? super T, ? extends K> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        return Gatherer.ofSequential(
                () -> new Object() { final Set<K> seen = new LinkedHashSet<>(); },
                (state, element, downstream) -> {
                    K key = keyExtractor.apply(element);
                    if (state.seen.add(key)) {
                        return downstream.push(element);
                    }
                    return true;
                }
        );
    }

    // ─────────────────────────────────────────────
    //  TakeWhile / DropWhile (Scala takeWhile / dropWhile)
    // ─────────────────────────────────────────────

    /**
     * Take elements while the predicate holds; stop at first failure.
     *
     * <p>Scala: {@code list.takeWhile(pred)}</p>
     * <p>Example: {@code [1,2,3,4,1,2].takeWhile(_ < 3) → [1,2]}</p>
     *
     * @param predicate  condition to keep taking elements
     * @return a Gatherer that stops forwarding once predicate fails
     */
    public static <T> Gatherer<T, ?, T> takeWhile(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    if (predicate.test(element)) {
                        return downstream.push(element);
                    }
                    return false; // short-circuit: stop processing
                }
        );
    }

    /**
     * Drop elements while the predicate holds; start forwarding once it fails.
     *
     * <p>Scala: {@code list.dropWhile(pred)}</p>
     * <p>Example: {@code [1,2,3,4,1,2].dropWhile(_ < 3) → [3,4,1,2]}</p>
     *
     * @param predicate  condition to skip elements
     * @return a Gatherer that drops elements until predicate fails
     */
    public static <T> Gatherer<T, ?, T> dropWhile(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        return Gatherer.ofSequential(
                () -> new Object() { boolean dropping = true; },
                (state, element, downstream) -> {
                    if (state.dropping && predicate.test(element)) {
                        return true; // skip
                    }
                    state.dropping = false;
                    return downstream.push(element);
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Partition (Scala partition)
    // ─────────────────────────────────────────────

    /**
     * Partition elements into two groups based on a predicate.
     * Emits a single {@link PartitionResult} containing both lists.
     *
     * <p>Scala: {@code list.partition(pred)} → (matching, nonMatching)</p>
     * <p>Example: {@code [1,2,3,4,5].partition(_ % 2 == 0) → ([2,4],[1,3,5])}</p>
     *
     * @param predicate  classifier predicate
     * @return a Gatherer that emits one PartitionResult at the end
     */
    public static <T> Gatherer<T, ?, PartitionResult<T>> partition(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final List<T> matching = new ArrayList<>();
                    final List<T> nonMatching = new ArrayList<>();
                },
                (state, element, downstream) -> {
                    if (predicate.test(element)) {
                        state.matching.add(element);
                    } else {
                        state.nonMatching.add(element);
                    }
                    return true;
                },
                (state, downstream) -> {
                    downstream.push(new PartitionResult<>(
                            Collections.unmodifiableList(new ArrayList<>(state.matching)),
                            Collections.unmodifiableList(new ArrayList<>(state.nonMatching))
                    ));
                }
        );
    }

    // ─────────────────────────────────────────────
    //  FlatMap (Scala flatMap)
    // ─────────────────────────────────────────────

    /**
     * One-to-many transformation: each element is expanded into zero or more outputs.
     *
     * <p>Scala: {@code list.flatMap(fn)}</p>
     * <p>Example: {@code [1,2,3].flatMap(x → [x, x*10]) → [1,10,2,20,3,30]}</p>
     *
     * @param mapper  function producing an Iterable of outputs per input
     * @return a Gatherer that flattens each mapped result into the stream
     */
    public static <T, R> Gatherer<T, ?, R> flatMap(Function<? super T, ? extends Iterable<? extends R>> mapper) {
        Objects.requireNonNull(mapper);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    Iterable<? extends R> iterable = Objects.requireNonNull(mapper.apply(element),
                            "flatMap mapper returned null");
                    for (R r : iterable) {
                        if (!downstream.push(r)) return false;
                    }
                    return true;
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Collect (Scala collect / PartialFunction)
    // ─────────────────────────────────────────────

    /**
     * Filter and map simultaneously: apply the function only when the predicate matches.
     *
     * <p>Scala: {@code list.collect { case x if cond(x) => f(x) }}</p>
     * <p>Example: {@code [1,2,3,4].collect(x → x*10 if x%2==0) → [20,40]}</p>
     *
     * @param predicate  filter condition
     * @param mapper     transformation for passing elements
     * @return a Gatherer that emits transformed elements passing the filter
     */
    public static <T, R> Gatherer<T, ?, R> collect(Predicate<? super T> predicate,
                                                    Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(mapper);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    if (predicate.test(element)) {
                        return downstream.push(mapper.apply(element));
                    }
                    return true;
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Peek (Vavr peek)
    // ─────────────────────────────────────────────

    /**
     * Observe each element with a side-effect, then pass it through unchanged.
     *
     * <p>Vavr: {@code list.peek(consumer)}</p>
     *
     * @param action  side-effect to perform on each element
     * @return a Gatherer that passes elements through after the action
     */
    public static <T> Gatherer<T, ?, T> peek(Consumer<? super T> action) {
        Objects.requireNonNull(action);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    action.accept(element);
                    return downstream.push(element);
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Prepend / Append (Vavr prepend / append)
    // ─────────────────────────────────────────────

    /**
     * Prepend a single element before the stream.
     *
     * <p>Vavr: {@code list.prepend(element)}</p>
     *
     * @param element  element to prepend
     * @return a Gatherer that emits the element first, then the stream
     */
    public static <T> Gatherer<T, ?, T> prepend(T element) {
        return Gatherer.ofSequential(
                () -> new Object() { boolean emitted = false; },
                (state, t, downstream) -> {
                    if (!state.emitted) {
                        if (!downstream.push(element)) return false;
                        state.emitted = true;
                    }
                    return downstream.push(t);
                },
                (state, downstream) -> {
                    // If stream was empty, still emit the prepended element
                    if (!state.emitted) {
                        if (!downstream.push(element)) return;
                    }
                }
        );
    }

    /**
     * Append a single element after the stream.
     *
     * <p>Vavr: {@code list.append(element)}</p>
     *
     * @param element  element to append
     * @return a Gatherer that emits the stream, then the element
     */
    public static <T> Gatherer<T, ?, T> append(T element) {
        return Gatherer.ofSequential(
                (_, t, downstream) -> downstream.push(t),
                (_, downstream) -> downstream.push(element)
        );
    }

    // ─────────────────────────────────────────────
    //  Cycle (Scala / Vavr cycle)
    // ─────────────────────────────────────────────

    /**
     * Cycle through the stream elements repeatedly, up to a maximum count.
     *
     * <p>Scala: {@code Iterator.continually(list).flatten.take(n)}</p>
     * <p>Example: {@code [1,2,3].cycle(7) → [1,2,3,1,2,3,1]}</p>
     *
     * @param times  maximum number of total elements to emit
     * @return a Gatherer that repeats the buffered elements
     */
    public static <T> Gatherer<T, ?, T> cycle(int times) {
        if (times < 0) throw new IllegalArgumentException("times must be >= 0, got " + times);
        return Gatherer.ofSequential(
                () -> new Object() { final List<T> buffer = new ArrayList<>(); },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    return true;
                },
                (state, downstream) -> {
                    if (state.buffer.isEmpty()) return;
                    int emitted = 0;
                    while (emitted < times) {
                        for (T e : state.buffer) {
                            if (emitted >= times) return;
                            if (!downstream.push(e)) return;
                            emitted++;
                        }
                    }
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Interleave (Vavr interleave)
    // ─────────────────────────────────────────────

    /**
     * Interleave elements from another iterable, alternating between stream and other.
     * If one source is longer, remaining elements are appended.
     *
     * <p>Vavr: {@code list1.interleave(list2)}</p>
     * <p>Example: {@code [1,3,5].interleave([2,4]) → [1,2,3,4,5]}</p>
     *
     * @param other  the other iterable to interleave with
     * @return a Gatherer that interleaves elements
     */
    public static <T> Gatherer<T, ?, T> interleave(Iterable<? extends T> other) {
        Objects.requireNonNull(other);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final Iterator<? extends T> otherIter = other.iterator();
                },
                (state, element, downstream) -> {
                    if (!downstream.push(element)) return false;
                    if (state.otherIter.hasNext()) {
                        return downstream.push(state.otherIter.next());
                    }
                    return true;
                },
                (state, downstream) -> {
                    while (state.otherIter.hasNext()) {
                        if (!downstream.push(state.otherIter.next())) return;
                    }
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Fold (Scala foldLeft — terminal-style as Gatherer)
    // ─────────────────────────────────────────────

    /**
     * Fold all elements into a single result, emitted once at the end.
     * Unlike {@link java.util.stream.Gatherers#fold}, this does NOT emit the initial value.
     *
     * <p>Scala: {@code list.foldLeft(z)(op)}</p>
     * <p>Example: {@code [1,2,3].foldLeft(0, +) → 6}</p>
     *
     * @param zero  initial value
     * @param op    accumulator function
     * @return a Gatherer that emits a single folded result
     */
    public static <T, A> Gatherer<T, ?, A> foldLeft(A zero, BiFunction<A, T, A> op) {
        Objects.requireNonNull(op);
        return Gatherer.ofSequential(
                () -> new Object() { A acc = zero; },
                (state, element, downstream) -> {
                    state.acc = op.apply(state.acc, element);
                    return true;
                },
                (state, downstream) -> { if (!downstream.push(state.acc)) return; }
        );
    }

    // ─────────────────────────────────────────────
    //  Reduce (Scala reduceLeft)
    // ─────────────────────────────────────────────

    /**
     * Reduce all elements into a single result using the given operator.
     * Emits {@link Optional#empty()} if the stream has no elements.
     *
     * <p>Scala: {@code list.reduceLeft(op)}</p>
     * <p>Example: {@code [1,2,3].reduceLeft((a,b) → a+b) → Optional[6]}</p>
     *
     * @param op  binary operator
     * @return a Gatherer that emits Optional of the reduced value
     */
    public static <T> Gatherer<T, ?, Optional<T>> reduceLeft(BinaryOperator<T> op) {
        Objects.requireNonNull(op);
        return Gatherer.ofSequential(
                () -> new Object() {
                    T acc = null;
                    boolean hasValue = false;
                },
                (state, element, downstream) -> {
                    if (!state.hasValue) {
                        state.acc = element;
                        state.hasValue = true;
                    } else {
                        state.acc = op.apply(state.acc, element);
                    }
                    return true;
                },
                (state, downstream) -> downstream.push(
                        state.hasValue ? Optional.ofNullable(state.acc) : Optional.empty()
                )
        );
    }

    // ─────────────────────────────────────────────
    //  Reverse (Scala reverse)
    // ─────────────────────────────────────────────

    /**
     * Reverse the order of elements in the stream.
     *
     * <p>Scala: {@code list.reverse}</p>
     * <p>Example: {@code [1,2,3].reverse → [3,2,1]}</p>
     *
     * @return a Gatherer that emits elements in reverse order
     */
    public static <T> Gatherer<T, ?, T> reverse() {
        return Gatherer.ofSequential(
                () -> new Object() { final List<T> buffer = new ArrayList<>(); },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    return true;
                },
                (state, downstream) -> {
                    for (int i = state.buffer.size() - 1; i >= 0; i--) {
                        if (!downstream.push(state.buffer.get(i))) return;
                    }
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Slice (Scala slice)
    // ─────────────────────────────────────────────

    /**
     * Take a slice of the stream from {@code fromIndex} (inclusive) to {@code toIndex} (exclusive).
     *
     * <p>Scala: {@code list.slice(from, until)}</p>
     * <p>Example: {@code [1,2,3,4,5].slice(1,4) → [2,3,4]}</p>
     *
     * @param fromIndex  start index (inclusive), 0-based
     * @param toIndex    end index (exclusive)
     * @return a Gatherer that emits only elements in the range
     */
    public static <T> Gatherer<T, ?, T> slice(int fromIndex, int toIndex) {
        if (fromIndex < 0) throw new IllegalArgumentException("fromIndex must be >= 0, got " + fromIndex);
        if (toIndex < fromIndex) throw new IllegalArgumentException("toIndex must be >= fromIndex, got toIndex=" + toIndex + " fromIndex=" + fromIndex);
        return Gatherer.ofSequential(
                () -> new Object() { int index = 0; },
                (state, element, downstream) -> {
                    if (state.index >= toIndex) return false; // short-circuit
                    if (state.index >= fromIndex) {
                        boolean pushed = downstream.push(element);
                        state.index++;
                        return pushed;
                    }
                    state.index++;
                    return true;
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Zip (Scala zip)
    // ─────────────────────────────────────────────

    /**
     * Pair elements with elements from another Iterable, stopping at the shorter length.
     *
     * <p>Scala: {@code list1.zip(list2)}</p>
     * <p>Example: {@code [1,2,3].zip(["a","b"]) → [(1,"a"),(2,"b")]}</p>
     *
     * @param other  the other iterable to zip with
     * @return a Gatherer emitting {@link Map.Entry} pairs
     */
    public static <T, U> Gatherer<T, ?, Map.Entry<T, U>> zip(Iterable<? extends U> other) {
        Objects.requireNonNull(other);
        return Gatherer.ofSequential(
                () -> new Object() { final Iterator<? extends U> otherIter = other.iterator(); },
                (state, element, downstream) -> {
                    if (!state.otherIter.hasNext()) return false; // stop when other runs out
                    return downstream.push(new AbstractMap.SimpleImmutableEntry<>(element, state.otherIter.next()));
                }
        );
    }

    // ─────────────────────────────────────────────
    //  Unfold (Scala unfold)
    // ─────────────────────────────────────────────

    /**
     * Generate a stream from a seed via an unfolding function.
     * This is a source Gatherer — it ignores input elements and emits
     * values generated by repeatedly applying the unfold function.
     *
     * <p>Scala: {@code Seq.unfold(seed)(s => Some((value, nextState)))}</p>
     * <p>Example: {@code unfold(1, s -> s <= 5 ? Optional.of(Map.entry(s, s+1)) : Optional.empty())}
     * → [1,2,3,4,5]</p>
     *
     * @param seed    initial state
     * @param unfold  function returning Optional of (output, next state); empty stops
     * @return a Gatherer that generates elements by unfolding
     */
    public static <T, S> Gatherer<T, ?, T> unfold(S seed,
            Function<? super S, Optional<Map.Entry<T, S>>> unfold) {
        Objects.requireNonNull(unfold);
        return Gatherer.ofSequential(
                (_, element, downstream) -> true, // ignore input
                (_, downstream) -> {
                    S state = seed;
                    while (true) {
                        Optional<Map.Entry<T, S>> next = unfold.apply(state);
                        if (next.isEmpty()) return;
                        Map.Entry<T, S> entry = next.get();
                        if (!downstream.push(entry.getKey())) return;
                        state = entry.getValue();
                    }
                }
        );
    }

    // ─────────────────────────────────────────────
    //  GroupBy (Scala groupBy)
    // ─────────────────────────────────────────────

    /**
     * Group elements by a key selector, emitting a single Map at the end.
     * Values for each key are collected in encounter order.
     *
     * <p>Scala: {@code list.groupBy(keyExtractor)}</p>
     * <p>Example: {@code ["aa","bb","ab"].groupBy(String::length) → {2: ["aa","bb","ab"]}}</p>
     *
     * @param keyExtractor  function to compute the grouping key
     * @return a Gatherer that emits one Map of grouped lists
     */
    public static <T, K> Gatherer<T, ?, Map<K, List<T>>> groupBy(Function<? super T, ? extends K> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        return Gatherer.ofSequential(
                () -> new Object() { final LinkedHashMap<K, List<T>> map = new LinkedHashMap<>(); },
                (state, element, downstream) -> {
                    state.map.computeIfAbsent(keyExtractor.apply(element), k -> new ArrayList<>()).add(element);
                    return true;
                },
                (state, downstream) -> {
                    Map<K, List<T>> result = new LinkedHashMap<>();
                    state.map.forEach((k, v) -> result.put(k,
                            Collections.unmodifiableList(new ArrayList<>(v))));
                    downstream.push(Collections.unmodifiableMap(result));
                }
        );
    }
}
