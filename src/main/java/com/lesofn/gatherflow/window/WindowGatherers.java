package com.lesofn.gatherflow.window;

import java.util.*;
import java.util.function.*;
import java.util.stream.Gatherer;

/**
 * Flink-inspired Stream Gatherer operators.
 *
 * <p>Simulates Apache Flink's core stream processing abstractions using
 * Java 25's {@link Gatherer} API (JEP 485). While Java Streams are bounded
 * and pull-based (unlike Flink's unbounded push-based model), these operators
 * faithfully replicate Flink's <em>semantics</em> on bounded data sets.</p>
 *
 * <h3>Feasibility analysis — Flink vs Gatherer:</h3>
 * <table>
 *   <tr><th>Flink Concept</th><th>Gatherer Feasibility</th><th>Notes</th></tr>
 *   <tr><td>Tumbling Window</td><td>✅ Full</td><td>Count-based exact match</td></tr>
 *   <tr><td>Sliding Window</td><td>✅ Full</td><td>Count-based with step</td></tr>
 *   <tr><td>Session Window</td><td>✅ Full</td><td>Gap-based, bounded stream</td></tr>
 *   <tr><td>Global Window</td><td>✅ Full</td><td>All elements in one window</td></tr>
 *   <tr><td>KeyBy</td><td>✅ Full</td><td>Partition by key</td></tr>
 *   <tr><td>Window Reduce/Aggregate</td><td>✅ Full</td><td>Apply function per window</td></tr>
 *   <tr><td>ProcessWindowFunction</td><td>✅ Full</td><td>Full window context access</td></tr>
 *   <tr><td>Union</td><td>✅ Full</td><td>Concat streams</td></tr>
 *   <tr><td>Connect / CoMap</td><td>✅ Partial</td><td>Two-type connect via tagged union</td></tr>
 *   <tr><td>Split / Side Output</td><td>✅ Partial</td><td>Route by predicate, emit tagged</td></tr>
 *   <tr><td>Event Time / Watermark</td><td>❌ Not feasible</td><td>Requires unbounded push model</td></tr>
 *   <tr><td>Checkpointing / Savepoint</td><td>❌ Not applicable</td><td>JVM-local, no distributed state</td></tr>
 *   <tr><td>Exactly-once / Fault Tolerance</td><td>❌ Not applicable</td><td>Single JVM, no retry</td></tr>
 *   <tr><td>Unbounded Source</td><td>❌ Not feasible</td><td>Java Streams are bounded</td></tr>
 * </table>
 *
 * <h3>Key design decisions:</h3>
 * <ul>
 *   <li>All window operators emit {@link Window} records carrying metadata
 *       (window ID, start/end index, contents) — mirroring Flink's
 *       {@code ProcessWindowFunction} context.</li>
 *   <li>Time-based windows use <em>element-local timestamps</em> rather than
 *       wall-clock time, allowing deterministic testing.</li>
 *   <li>Session windows use a <em>gap threshold</em> between consecutive element
 *       timestamps (or indices) to delineate sessions.</li>
 * </ul>
 */
public final class WindowGatherers {

    private WindowGatherers() {}

    // ═══════════════════════════════════════════════
    //  Tumbling Window (Flink: count-based tumbling)
    // ═══════════════════════════════════════════════

    /**
     * Tumbling count window: non-overlapping fixed-size windows.
     *
     * <p>Flink: {@code stream.keyBy(...).countWindow(size)}</p>
     * <p>Example: {@code [1,2,3,4,5].tumblingWindow(2) →
     *   Window(id=0,0-1,[1,2]), Window(id=1,2-3,[3,4]), Window(id=2,4-4,[5])}</p>
     *
     * @param size  window size (element count), must be >= 1
     * @return a Gatherer emitting {@link Window} records
     */
    public static <T> Gatherer<T, ?, Window<T>> tumblingWindow(int size) {
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final List<T> buffer = new ArrayList<>();
                    long windowId = 0;
                    long globalIndex = 0;
                    long windowStartIndex = 0;
                },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    if (state.buffer.size() == size) {
                        if (!downstream.push(new Window<>(
                                state.windowId,
                                state.windowStartIndex,
                                state.globalIndex,
                                state.buffer
                        ))) return false;
                        state.buffer.clear();
                        state.windowId++;
                        state.windowStartIndex = state.globalIndex + 1;
                    }
                    state.globalIndex++;
                    return true;
                },
                (state, downstream) -> {
                    if (!state.buffer.isEmpty()) {
                        if (!downstream.push(new Window<>(
                                state.windowId,
                                state.windowStartIndex,
                                state.globalIndex - 1,
                                state.buffer
                        ))) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Sliding Window (Flink: count-based sliding)
    // ═══════════════════════════════════════════════

    /**
     * Sliding count window with step 1 (overlapping).
     *
     * <p>Flink: {@code stream.keyBy(...).countWindow(size, slide)}</p>
     * <p>Example: {@code [1,2,3,4,5].slidingWindow(3) →
     *   Window(0-2,[1,2,3]), Window(1-3,[2,3,4]), Window(2-4,[3,4,5])}</p>
     *
     * @param size  window size
     * @return a Gatherer emitting overlapping {@link Window} records
     */
    public static <T> Gatherer<T, ?, Window<T>> slidingWindow(int size) {
        return slidingWindow(size, 1);
    }

    /**
     * Sliding count window with configurable step.
     *
     * <p>Flink: {@code stream.keyBy(...).countWindow(size, slide)}</p>
     * <p>Example: {@code [1,2,3,4,5].slidingWindow(3,2) →
     *   Window(0-2,[1,2,3]), Window(2-4,[3,4,5])}</p>
     *
     * @param size  window size, must be >= 1
     * @param slide  step between window starts, must be >= 1
     * @return a Gatherer emitting sliding {@link Window} records
     */
    public static <T> Gatherer<T, ?, Window<T>> slidingWindow(int size, int slide) {
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        if (slide < 1) throw new IllegalArgumentException("slide must be >= 1, got " + slide);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final ArrayDeque<T> buffer = new ArrayDeque<>();
                    long bufferStartIndex = 0;
                    long globalIndex = 0;
                    long windowId = 0;
                    long nextWindowStart = 0;
                },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    while (state.nextWindowStart + size - 1 <= state.globalIndex) {
                        int relStart = (int) (state.nextWindowStart - state.bufferStartIndex);
                        List<T> windowElements = new ArrayList<>(size);
                        int idx = 0;
                        for (T e : state.buffer) {
                            if (idx >= relStart + size) break;
                            if (idx >= relStart) windowElements.add(e);
                            idx++;
                        }
                        long endIndex = state.nextWindowStart + size - 1;
                        if (!downstream.push(new Window<>(
                                state.windowId,
                                state.nextWindowStart,
                                endIndex,
                                windowElements
                        ))) return false;
                        state.windowId++;
                        state.nextWindowStart += slide;
                        while (!state.buffer.isEmpty() && state.bufferStartIndex < state.nextWindowStart) {
                            state.buffer.pollFirst();
                            state.bufferStartIndex++;
                        }
                    }
                    state.globalIndex++;
                    return true;
                },
                (state, downstream) -> {
                    if (slide > 1 && state.nextWindowStart < state.globalIndex) {
                        int relStart = (int) (state.nextWindowStart - state.bufferStartIndex);
                        if (relStart < state.buffer.size()) {
                            List<T> windowElements = new ArrayList<>();
                            int idx = 0;
                            for (T e : state.buffer) {
                                if (idx >= relStart) windowElements.add(e);
                                idx++;
                            }
                            long endIndex = state.globalIndex - 1;
                            if (!downstream.push(new Window<>(
                                    state.windowId,
                                    state.nextWindowStart,
                                    endIndex,
                                    windowElements
                            ))) return;
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Session Window (Flink: gap-based session window)
    // ═══════════════════════════════════════════════

    /**
     * Session window based on a gap threshold between consecutive elements.
     * A new session starts when the gap between the current element's timestamp
     * and the previous element's timestamp exceeds the specified gap.
     *
     * <p>Flink: {@code stream.keyBy(...).window(EventTimeSessionWindows.withGap(gap))}</p>
     * <p>Example (timestamps in ms): {@code [(1,"a"),(2,"b"),(10,"c"),(11,"d")].sessionWindow(5, t→t._1)}
     *   → Session(0,[a,b]), Session(1,[c,d])</p>
     *
     * @param gap              maximum gap between consecutive elements in the same session
     * @param timestampExtractor  function to extract a timestamp (long) from each element
     * @return a Gatherer emitting session {@link Window} records
     */
    public static <T> Gatherer<T, ?, Window<T>> sessionWindow(long gap,
            ToLongFunction<? super T> timestampExtractor) {
        if (gap < 0) throw new IllegalArgumentException("gap must be >= 0, got " + gap);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final List<T> buffer = new ArrayList<>();
                    long windowId = 0;
                    long windowStartIndex = 0;
                    long lastTimestamp;
                    long globalIndex = 0;
                },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    if (!state.buffer.isEmpty()
                            && ts > state.lastTimestamp
                            && Long.compareUnsigned(ts - state.lastTimestamp, gap) > 0) {
                        // Gap exceeded — emit current session and start new one
                        if (!downstream.push(new Window<>(
                                state.windowId,
                                state.windowStartIndex,
                                state.globalIndex - 1,
                                state.buffer
                        ))) return false;
                        state.buffer.clear();
                        state.windowId++;
                        state.windowStartIndex = state.globalIndex;
                    }
                    state.buffer.add(element);
                    state.lastTimestamp = ts;
                    state.globalIndex++;
                    return true;
                },
                (state, downstream) -> {
                    if (!state.buffer.isEmpty()) {
                        if (!downstream.push(new Window<>(
                                state.windowId,
                                state.windowStartIndex,
                                state.globalIndex - 1,
                                state.buffer
                        ))) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Global Window (Flink: GlobalWindows)
    // ═══════════════════════════════════════════════

    /**
     * Global window: all elements belong to a single window.
     *
     * <p>Flink: {@code stream.keyBy(...).window(GlobalWindows.create())}
     * — typically used with a custom Trigger.</p>
     * <p>Example: {@code [1,2,3,4,5].globalWindow() → Window(0,0-4,[1,2,3,4,5])}</p>
     *
     * @return a Gatherer emitting a single {@link Window} containing all elements
     */
    public static <T> Gatherer<T, ?, Window<T>> globalWindow() {
        return Gatherer.ofSequential(
                () -> new Object() {
                    final List<T> buffer = new ArrayList<>();
                    long globalIndex = 0;
                },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    state.globalIndex++;
                    return true;
                },
                (state, downstream) -> {
                    if (!state.buffer.isEmpty()) {
                        if (!downstream.push(new Window<>(0, 0, state.globalIndex - 1, state.buffer))) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Windowed Reduce (Flink: .reduce() on WindowedStream)
    // ═══════════════════════════════════════════════

    /**
     * Apply a reduce function to each window emitted by a window Gatherer.
     *
     * <p>Flink: {@code windowedStream.reduce(reduceFunction)}</p>
     * <p>Example: {@code stream.gather(tumblingWindow(2)).gather(windowReduce(Integer::sum))}
     *   → [3, 7, 5]</p>
     *
     * @param reducer  binary operator to reduce window elements
     * @return a Gatherer that reduces each Window to a single value
     */
    public static <T> Gatherer<Window<T>, ?, T> windowReduce(BinaryOperator<T> reducer) {
        Objects.requireNonNull(reducer);
        return Gatherer.ofSequential(
                (_, window, downstream) -> {
                    if (window.isEmpty()) return true;
                    T acc = window.elements().getFirst();
                    for (int i = 1; i < window.elements().size(); i++) {
                        acc = reducer.apply(acc, window.elements().get(i));
                    }
                    return downstream.push(acc);
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Windowed Aggregate (Flink: .aggregate() on WindowedStream)
    // ═══════════════════════════════════════════════

    /**
     * Apply an aggregate function to each window, with a separate accumulator type.
     *
     * <p>Flink: {@code windowedStream.aggregate(aggregateFunction)}</p>
     * <p>Mirrors Flink's {@code AggregateFunction<IN, ACC, OUT>} with three functions:</p>
     * <ul>
     *   <li>{@code createAccumulator} — initial accumulator</li>
     *   <li>{@code add} — add element to accumulator</li>
     *   <li>{@code getResult} — extract result from accumulator</li>
     * </ul>
     *
     * @param createAcc  supplier for initial accumulator
     * @param add        accumulator adder
     * @param getResult  result extractor
     * @return a Gatherer that aggregates each Window to a result
     */
    public static <T, A, R> Gatherer<Window<T>, ?, R> windowAggregate(
            Supplier<A> createAcc,
            BiFunction<A, T, A> add,
            Function<A, R> getResult) {
        Objects.requireNonNull(createAcc);
        Objects.requireNonNull(add);
        Objects.requireNonNull(getResult);
        return Gatherer.ofSequential(
                (_, window, downstream) -> {
                    A acc = createAcc.get();
                    for (T element : window.elements()) {
                        acc = add.apply(acc, element);
                    }
                    return downstream.push(getResult.apply(acc));
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ProcessWindowFunction (Flink: .process() on WindowedStream)
    // ═══════════════════════════════════════════════

    /**
     * Apply a full ProcessWindowFunction with access to window metadata.
     *
     * <p>Flink: {@code windowedStream.process(new ProcessWindowFunction() {...})}</p>
     * <p>The processor receives the {@link Window} (with metadata) and can emit
     * zero or more results — enabling the most flexible window computation.</p>
     *
     * @param processor  function from Window to Iterable of results
     * @return a Gatherer that processes each Window into result(s)
     */
    public static <T, R> Gatherer<Window<T>, ?, R> windowProcess(
            Function<? super Window<T>, ? extends Iterable<? extends R>> processor) {
        Objects.requireNonNull(processor);
        return Gatherer.ofSequential(
                (_, window, downstream) -> {
                    for (R r : processor.apply(window)) {
                        if (!downstream.push(r)) return false;
                    }
                    return true;
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  KeyBy (Flink: keyBy)
    // ═══════════════════════════════════════════════

    /**
     * Tag each element with its key, analogous to Flink's {@code keyBy()}.
     * The result is a {@link KeyedResult} that pairs each element with its key.
     *
     * <p>Flink: {@code stream.keyBy(element -> key)}</p>
     * <p>Example: {@code ["aa","bb","c"].keyBy(String::length) →
     *   [("aa",2), ("bb",2), ("c",1)]}</p>
     *
     * @param keyExtractor  function to extract the key
     * @return a Gatherer emitting {@link KeyedResult} of key and element
     */
    public static <T, K> Gatherer<T, ?, KeyedResult<K, T>> keyBy(
            Function<? super T, ? extends K> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    K key = keyExtractor.apply(element);
                    return downstream.push(new KeyedResult<>(key, element));
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Keyed Tumbling Window (Flink: keyBy + countWindow)
    // ═══════════════════════════════════════════════

    /**
     * Keyed tumbling count window: elements are grouped by key, then each key's
     * elements are windowed independently.
     *
     * <p>Flink: {@code stream.keyBy(f).countWindow(size)}</p>
     * <p>Example: {@code [(1,"a"),(2,"b"),(1,"c"),(2,"d")].keyedTumblingWindow(key, 2)}
     *   → KeyedResult(key=1, Window([a,c])), KeyedResult(key=2, Window([b,d]))</p>
     *
     * @param keyExtractor  function to extract the key
     * @param size          window size per key
     * @return a Gatherer emitting {@link KeyedResult} of key and Window
     */
    public static <T, K> Gatherer<T, ?, KeyedResult<K, Window<T>>> keyedTumblingWindow(
            Function<? super T, ? extends K> keyExtractor, int size) {
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        Objects.requireNonNull(keyExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final LinkedHashMap<K, List<T>> keyedBuffers = new LinkedHashMap<>();
                    final LinkedHashMap<K, Long> keyedWindowIds = new LinkedHashMap<>();
                    final LinkedHashMap<K, Long> keyedIndices = new LinkedHashMap<>();
                },
                (state, element, downstream) -> {
                    K key = keyExtractor.apply(element);
                    state.keyedBuffers.computeIfAbsent(key, k -> new ArrayList<>()).add(element);
                    long idx = state.keyedIndices.getOrDefault(key, 0L);
                    long windowId = state.keyedWindowIds.getOrDefault(key, 0L);

                    List<T> buffer = state.keyedBuffers.get(key);
                    if (buffer.size() == size) {
                        if (!downstream.push(new KeyedResult<>(key, new Window<>(
                                windowId, idx - size + 1, idx, buffer)))) return false;
                        buffer.clear();
                        state.keyedWindowIds.put(key, windowId + 1);
                    }
                    state.keyedIndices.put(key, idx + 1);
                    return true;
                },
                (state, downstream) -> {
                    // Emit remaining partial windows per key
                    for (var entry : state.keyedBuffers.entrySet()) {
                        K key = entry.getKey();
                        List<T> buffer = entry.getValue();
                        if (!buffer.isEmpty()) {
                            long idx = state.keyedIndices.getOrDefault(key, 0L);
                            long windowId = state.keyedWindowIds.getOrDefault(key, 0L);
                            if (!downstream.push(new KeyedResult<>(key, new Window<>(
                                    windowId, idx - buffer.size(), idx - 1, buffer)))) return;
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Keyed Window Reduce (Flink: keyBy + window + reduce)
    // ═══════════════════════════════════════════════

    /**
     * Apply a reduce function within each keyed window.
     *
     * <p>Flink: {@code stream.keyBy(f).countWindow(n).reduce(reducer)}</p>
     *
     * @param reducer  binary operator to reduce elements within each key's window
     * @return a Gatherer that reduces each keyed window to a single result
     */
    public static <T, K> Gatherer<KeyedResult<K, Window<T>>, ?, KeyedResult<K, T>> keyedWindowReduce(
            BinaryOperator<T> reducer) {
        Objects.requireNonNull(reducer);
        return Gatherer.ofSequential(
                (_, keyedWindow, downstream) -> {
                    Window<T> window = keyedWindow.result();
                    if (window.isEmpty()) return true;
                    T acc = window.elements().getFirst();
                    for (int i = 1; i < window.elements().size(); i++) {
                        acc = reducer.apply(acc, window.elements().get(i));
                    }
                    return downstream.push(new KeyedResult<>(keyedWindow.key(), acc));
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Keyed Window Aggregate (Flink: keyBy + window + aggregate)
    // ═══════════════════════════════════════════════

    /**
     * Apply an aggregate function within each keyed window.
     *
     * <p>Flink: {@code stream.keyBy(f).countWindow(n).aggregate(aggFunc)}</p>
     *
     * @param createAcc  supplier for initial accumulator
     * @param add        accumulator adder
     * @param getResult  result extractor
     * @return a Gatherer that aggregates each keyed window
     */
    public static <T, K, A, R> Gatherer<KeyedResult<K, Window<T>>, ?, KeyedResult<K, R>> keyedWindowAggregate(
            Supplier<A> createAcc,
            BiFunction<A, T, A> add,
            Function<A, R> getResult) {
        Objects.requireNonNull(createAcc);
        Objects.requireNonNull(add);
        Objects.requireNonNull(getResult);
        return Gatherer.ofSequential(
                (_, keyedWindow, downstream) -> {
                    A acc = createAcc.get();
                    for (T element : keyedWindow.result().elements()) {
                        acc = add.apply(acc, element);
                    }
                    return downstream.push(new KeyedResult<>(keyedWindow.key(), getResult.apply(acc)));
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Union (Flink: union)
    // ═══════════════════════════════════════════════

    /**
     * Union (concatenate) another stream's elements after the current stream.
     *
     * <p>Flink: {@code stream1.union(stream2)}</p>
     * <p>Example: {@code [1,2,3].union([4,5]) → [1,2,3,4,5]}</p>
     *
     * @param other  the other iterable to append
     * @return a Gatherer that appends the other iterable's elements
     */
    public static <T> Gatherer<T, ?, T> union(Iterable<? extends T> other) {
        Objects.requireNonNull(other);
        return Gatherer.ofSequential(
                (_, element, downstream) -> downstream.push(element),
                (_, downstream) -> {
                    for (T t : other) {
                        if (!downstream.push(t)) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Split / Side Output (Flink: split / side output)
    // ═══════════════════════════════════════════════

    /**
     * Split (route) elements into tagged groups based on a classifier function.
     * Each element is emitted as a {@link Tagged} record carrying its assigned tag.
     *
     * <p>Flink: {@code stream.split(element -> ...)} / {@code OutputTag}</p>
     * <p>Example: {@code [1,2,3,4].split(x -> x%2==0 ? "even" : "odd")}
     *   → [Tagged("odd",1), Tagged("even",2), Tagged("odd",3), Tagged("even",4)]</p>
     *
     * @param classifier  function assigning a tag to each element
     * @return a Gatherer emitting {@link Tagged} elements
     */
    public static <T> Gatherer<T, ?, Tagged<T>> split(Function<? super T, String> classifier) {
        Objects.requireNonNull(classifier);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    String tag = Objects.requireNonNull(classifier.apply(element),
                            "split classifier returned null tag");
                    return downstream.push(new Tagged<>(tag, element));
                }
        );
    }

    /**
     * Filter tagged elements by a specific tag — Flink's side-output selection.
     *
     * <p>Flink: {@code stream.getSideOutput(outputTag)}</p>
     *
     * @param tag  the tag to select
     * @return a Gatherer that only passes elements matching the tag
     */
    public static <T> Gatherer<Tagged<T>, ?, T> selectTag(String tag) {
        Objects.requireNonNull(tag);
        return Gatherer.ofSequential(
                (_, tagged, downstream) -> {
                    if (Objects.equals(tag, tagged.tag())) {
                        return downstream.push(tagged.value());
                    }
                    return true;
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Connect / CoMap (Flink: connect + map)
    // ═══════════════════════════════════════════════

    /**
     * Connect two streams by interleaving elements from another iterable,
     * tagging each with its source ("main" or "other").
     *
     * <p>Flink: {@code stream1.connect(stream2)}</p>
     * <p>After connecting, use {@code coMap} to process each type independently.</p>
     *
     * @param other  the other iterable to connect with
     * @return a Gatherer emitting {@link Tagged} elements with "main" or "other"
     */
    public static <T> Gatherer<T, ?, Tagged<T>> connect(Iterable<? extends T> other) {
        Objects.requireNonNull(other);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final Iterator<? extends T> otherIter = other.iterator();
                },
                (state, element, downstream) -> {
                    if (!downstream.push(new Tagged<>("main", element))) return false;
                    if (state.otherIter.hasNext()) {
                        if (!downstream.push(new Tagged<>("other", state.otherIter.next()))) return false;
                    }
                    return true;
                },
                (state, downstream) -> {
                    while (state.otherIter.hasNext()) {
                        if (!downstream.push(new Tagged<>("other", state.otherIter.next()))) return;
                    }
                }
        );
    }

    /**
     * Co-map: apply different functions to "main" and "other" tagged elements.
     *
     * <p>Flink: {@code connectedStreams.map(func1, func2)}</p>
     *
     * @param mainMapper  function for elements tagged "main"
     * @param otherMapper function for elements tagged "other"
     * @return a Gatherer that applies the appropriate function per tag
     */
    public static <T, R> Gatherer<Tagged<T>, ?, R> coMap(
            Function<? super T, ? extends R> mainMapper,
            Function<? super T, ? extends R> otherMapper) {
        Objects.requireNonNull(mainMapper);
        Objects.requireNonNull(otherMapper);
        return Gatherer.ofSequential(
                (_, tagged, downstream) -> {
                    if ("main".equals(tagged.tag())) {
                        return downstream.push(mainMapper.apply(tagged.value()));
                    } else if ("other".equals(tagged.tag())) {
                        return downstream.push(otherMapper.apply(tagged.value()));
                    } else {
                        throw new IllegalArgumentException("Unknown tag: " + tagged.tag());
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Time-based Window (Flink: processing-time / event-time window)
    // ═══════════════════════════════════════════════

    /**
     * Tumbling time window based on element-local timestamps.
     * Elements are assigned to windows based on their timestamp falling within
     * {@code [windowStart, windowStart + size)}.
     *
     * <p>Flink: {@code stream.keyBy(...).window(TumblingEventTimeWindows.of(size))}</p>
     * <p>Example: timestamps [0,50,100,150,200] with size=100 →
     *   Window(0-99,[e0,e50]), Window(100-199,[e100,e150]), Window(200-299,[e200])</p>
     *
     * <p><em>Note:</em> Unlike Flink, this operates on bounded streams with
     * element-local timestamps rather than wall-clock / watermark time.</p>
     *
     * @param size              window size in time units
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer emitting time-based tumbling {@link Window} records
     */
    public static <T> Gatherer<T, ?, Window<T>> tumblingTimeWindow(long size,
            ToLongFunction<? super T> timestampExtractor) {
        if (size <= 0) throw new IllegalArgumentException("size must be > 0, got " + size);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final TreeMap<Long, List<T>> windows = new TreeMap<>();
                    long windowId = 0;
                    long lastWindowStart = Long.MIN_VALUE;
                },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    long windowStart = Math.floorDiv(ts, size) * size; // align to size boundary
                    // When moving to a new window, emit all previous complete windows
                    if (state.lastWindowStart != Long.MIN_VALUE && windowStart > state.lastWindowStart) {
                        for (var entry : state.windows.entrySet()) {
                            long ws = entry.getKey();
                            long we = ws + size - 1;
                            if (!downstream.push(new Window<>(state.windowId, ws, we, entry.getValue()))) return false;
                            state.windowId++;
                        }
                        state.windows.clear();
                    }
                    state.windows.computeIfAbsent(windowStart, k -> new ArrayList<>()).add(element);
                    state.lastWindowStart = windowStart;
                    return true;
                },
                (state, downstream) -> {
                    for (var entry : state.windows.entrySet()) {
                        long ws = entry.getKey();
                        long we = ws + size - 1;
                        if (!downstream.push(new Window<>(state.windowId, ws, we, entry.getValue()))) return;
                        state.windowId++;
                    }
                }
        );
    }

    /**
     * Sliding time window based on element-local timestamps.
     *
     * <p>Flink: {@code stream.keyBy(...).window(SlidingEventTimeWindows.of(size, slide))}</p>
     * <p>Example: timestamps [0,50,100,150] with size=100, slide=50 →
     *   Window(0-99,[e0,e50]), Window(50-149,[e50,e100]), Window(100-199,[e100,e150])</p>
     *
     * @param size              window size in time units
     * @param slide             slide interval in time units
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer emitting time-based sliding {@link Window} records
     */
    public static <T> Gatherer<T, ?, Window<T>> slidingTimeWindow(long size, long slide,
            ToLongFunction<? super T> timestampExtractor) {
        if (size <= 0) throw new IllegalArgumentException("size must be > 0, got " + size);
        if (slide <= 0) throw new IllegalArgumentException("slide must be > 0, got " + slide);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final List<T> elements = new ArrayList<>();
                    final List<Long> timestamps = new ArrayList<>();
                },
                (state, element, downstream) -> {
                    state.elements.add(element);
                    state.timestamps.add(timestampExtractor.applyAsLong(element));
                    return true;
                },
                (state, downstream) -> {
                    int n = state.elements.size();
                    if (n == 0) return;
                    long[] timestamps = new long[n];
                    for (int i = 0; i < n; i++) {
                        timestamps[i] = state.timestamps.get(i);
                    }
                    Integer[] indices = new Integer[n];
                    Arrays.setAll(indices, i -> i);
                    Arrays.sort(indices, Comparator.comparingLong(i -> timestamps[i]));
                    long minTs = timestamps[indices[0]];
                    long maxTs = timestamps[indices[n - 1]];
                    long firstStart = Math.floorDiv(minTs, slide) * slide;
                    if (firstStart + size <= minTs) {
                        firstStart += slide;
                    }
                    int left = 0;
                    int right = 0;
                    long windowId = 0;
                    for (long s = firstStart; s <= maxTs; s += slide) {
                        long windowEnd = s + size - 1;
                        while (left < n && timestamps[indices[left]] < s) {
                            left++;
                        }
                        if (left == n) break;
                        while (right < n && timestamps[indices[right]] <= windowEnd) {
                            right++;
                        }
                        if (left < right) {
                            List<T> windowElements = new ArrayList<>(right - left);
                            for (int i = left; i < right; i++) {
                                windowElements.add(state.elements.get(indices[i]));
                            }
                            if (!downstream.push(new Window<>(windowId, s, windowEnd, windowElements))) break;
                            windowId++;
                        }
                        if (s > Long.MAX_VALUE - slide) break;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Apply / Transform (Flink: .map / .flatMap)
    // ═══════════════════════════════════════════════

    /**
     * Apply a function to each window, similar to Flink's {@code AllWindowedStream.apply()}.
     * Unlike {@link #windowProcess}, this provides a simpler (Window → R) interface.
     *
     * <p>Flink: {@code windowedStream.apply(windowFunction)}</p>
     *
     * @param function  function from Window to result
     * @return a Gatherer that applies the function to each Window
     */
    public static <T, R> Gatherer<Window<T>, ?, R> windowApply(
            Function<? super Window<T>, ? extends R> function) {
        Objects.requireNonNull(function);
        return Gatherer.ofSequential(
                (_, window, downstream) -> downstream.push(function.apply(window))
        );
    }

    // ═══════════════════════════════════════════════
    //  Window Count (Flink: .count() on WindowedStream)
    // ═══════════════════════════════════════════════

    /**
     * Count elements in each window.
     *
     * <p>Flink: {@code windowedStream.count()}</p>
     *
     * @return a Gatherer that emits the count of elements in each Window
     */
    public static <T> Gatherer<Window<T>, ?, Long> windowCount() {
        return Gatherer.ofSequential(
                (_, window, downstream) -> downstream.push((long) window.size())
        );
    }

    // ═══════════════════════════════════════════════
    //  Window Sum (Flink: .sum() on WindowedStream)
    // ═══════════════════════════════════════════════

    /**
     * Sum numeric elements in each window.
     *
     * <p>Flink: {@code windowedStream.sum(field)}</p>
     *
     * @param valueExtractor  function to extract a numeric value from each element
     * @return a Gatherer that emits the sum of each Window
     */
    public static <T> Gatherer<Window<T>, ?, Double> windowSum(ToDoubleFunction<? super T> valueExtractor) {
        Objects.requireNonNull(valueExtractor);
        return Gatherer.ofSequential(
                (_, window, downstream) -> {
                    double sum = 0;
                    for (T e : window.elements()) {
                        sum += valueExtractor.applyAsDouble(e);
                    }
                    return downstream.push(sum);
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Window Min / Max (Flink: .min() / .max() on WindowedStream)
    // ═══════════════════════════════════════════════

    /**
     * Find the minimum element in each window by comparator.
     *
     * <p>Flink: {@code windowedStream.min(field)}</p>
     *
     * @param comparator  comparator for ordering elements
     * @return a Gatherer that emits the min element of each Window
     */
    public static <T> Gatherer<Window<T>, ?, Optional<T>> windowMin(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator);
        return Gatherer.ofSequential(
                (_, window, downstream) -> {
                    T min = null;
                    boolean found = false;
                    for (T e : window.elements()) {
                        if (!found) {
                            min = e;
                            found = true;
                        } else if (comparator.compare(e, min) < 0) {
                            min = e;
                        }
                    }
                    return downstream.push(Optional.ofNullable(min));
                }
        );
    }

    /**
     * Find the maximum element in each window by comparator.
     *
     * <p>Flink: {@code windowedStream.max(field)}</p>
     *
     * @param comparator  comparator for ordering elements
     * @return a Gatherer that emits the max element of each Window
     */
    public static <T> Gatherer<Window<T>, ?, Optional<T>> windowMax(Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator);
        return Gatherer.ofSequential(
                (_, window, downstream) -> {
                    T max = null;
                    boolean found = false;
                    for (T e : window.elements()) {
                        if (!found) {
                            max = e;
                            found = true;
                        } else if (comparator.compare(e, max) > 0) {
                            max = e;
                        }
                    }
                    return downstream.push(Optional.ofNullable(max));
                }
        );
    }
}
