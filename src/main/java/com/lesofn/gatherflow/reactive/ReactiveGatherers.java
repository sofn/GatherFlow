package com.lesofn.gatherflow.reactive;

import java.util.*;
import java.util.function.*;
import java.util.stream.Gatherer;

/**
 * Reactive-style Stream Gatherer operators inspired by RxJava and Project Reactor.
 *
 * <p>Simulates RxJava/Reactor's core reactive abstractions using JDK 24's
 * {@link Gatherer} API. Only operators that <em>do not already exist</em> in
 * {@link java.util.stream.Stream} are implemented here.</p>
 *
 * <h3>Java Stream already provides (NOT re-implemented):</h3>
 * <ul>
 *   <li>{@code map} → {@link Stream#map}</li>
 *   <li>{@code filter} → {@link Stream#filter}</li>
 *   <li>{@code flatMap} → {@link Stream#flatMap}</li>
 *   <li>{@code distinct} → {@link Stream#distinct}</li>
 *   <li>{@code sorted} → {@link Stream#sorted}</li>
 *   <li>{@code peek} → {@link Stream#peek}</li>
 *   <li>{@code limit} → {@link Stream#limit}</li>
 *   <li>{@code skip} → {@link Stream#skip}</li>
 *   <li>{@code takeWhile} → {@link Stream#takeWhile}</li>
 *   <li>{@code dropWhile} → {@link Stream#dropWhile}</li>
 *   <li>{@code reduce} → {@link Stream#reduce}</li>
 *   <li>{@code count} → {@link Stream#count}</li>
 *   <li>{@code min/max} → {@link Stream#min}/{@link Stream#max}</li>
 *   <li>{@code forEach} → {@link Stream#forEach}</li>
 *   <li>{@code collect} → {@link Stream#collect}</li>
 *   <li>{@code findFirst/findAny} → {@link Stream#findFirst}/{@link Stream#findAny}</li>
 *   <li>{@code anyMatch/allMatch/noneMatch} → {@link Stream#anyMatch} etc.</li>
 *   <li>{@code concat} → {@link Stream#concat}</li>
 * </ul>
 *
 * <h3>Feasibility analysis — RxJava/Reactor vs Gatherer:</h3>
 * <table>
 *   <tr><th>Reactive Concept</th><th>Gatherer Feasibility</th><th>Notes</th></tr>
 *   <tr><td>debounce</td><td>✅ Full</td><td>Timestamp-based, bounded stream</td></tr>
 *   <tr><td>throttleFirst / throttleLast</td><td>✅ Full</td><td>Timestamp-based rate limiting</td></tr>
 *   <tr><td>sample (throttleLatest)</td><td>✅ Full</td><td>Take latest per time bucket</td></tr>
 *   <tr><td>buffer (count-based)</td><td>✅ Full</td><td>Same as grouped/chunk</td></tr>
 *   <tr><td>buffer (time-based)</td><td>✅ Full</td><td>Timestamp-based batching</td></tr>
 *   <tr><td>window (count-based)</td><td>✅ Full</td><td>Already in FlinkSequenceGatherers</td></tr>
 *   <tr><td>scan (with seed)</td><td>✅ Full</td><td>Already in SequenceGatherers</td></tr>
 *   <tr><td>timestamp / timeInterval</td><td>✅ Full</td><td>Add timing metadata</td></tr>
 *   <tr><td>delay</td><td>✅ Partial</td><td>Can reorder by timestamp, no wall-clock delay</td></tr>
 *   <tr><td>doOnNext / doOnComplete / doOnError</td><td>✅ Full</td><td>Side-effect hooks</td></tr>
 *   <tr><td>doFinally</td><td>✅ Full</td><td>Cleanup hook on stream end</td></tr>
 *   <tr><td>materialize / dematerialize</td><td>✅ Full</td><td>Wrap/unwrap stream notifications</td></tr>
 *   <tr><td>onErrorReturn</td><td>✅ Partial</td><td>Map exceptions to fallback values</td></tr>
 *   <tr><td>onErrorResume</td><td>✅ Partial</td><td>Switch to fallback stream on error</td></tr>
 *   <tr><td>retry</td><td>⚠️ Limited</td><td>Can retry map-like operations only</td></tr>
 *   <tr><td>repeat</td><td>✅ Full</td><td>Repeat bounded stream N times</td></tr>
 *   <tr><td>defaultIfEmpty</td><td>✅ Full</td><td>Emit default when stream is empty</td></tr>
 *   <tr><td>switchIfEmpty</td><td>✅ Full</td><td>Switch to fallback when stream is empty</td></tr>
 *   <tr><td>startWith</td><td>✅ Full</td><td>Prepend iterable</td></tr>
 *   <tr><td>concatWith</td><td>✅ Full</td><td>Append iterable</td></tr>
 *   <tr><td>combineLatest</td><td>❌ Not feasible</td><td>Requires push model + unbounded</td></tr>
 *   <tr><td>amb / race</td><td>❌ Not feasible</td><td>Requires async push model</td></tr>
 *   <tr><td>observeOn / subscribeOn</td><td>❌ Not feasible</td><td>Thread scheduling needs async runtime</td></tr>
 *   <tr><td>publish / refCount / share</td><td>❌ Not feasible</td><td>Hot-cold duality needs ConnectableObservable</td></tr>
 *   <tr><td>backpressure (buffer/loss)</td><td>❌ Not applicable</td><td>Pull-based Streams have natural backpressure</td></tr>
 *   <tr><td>timeout</td><td>❌ Not feasible</td><td>Requires wall-clock async</td></tr>
 *   <tr><td>interval</td><td>❌ Not feasible</td><td>Requires unbounded push source</td></tr>
 * </table>
 */
public final class ReactiveGatherers {

    private ReactiveGatherers() {}

    // ═══════════════════════════════════════════════
    //  Debounce (RxJava: debounce, Reactor: debounce)
    // ═══════════════════════════════════════════════

    /**
     * Emit only the last element in each time bucket where consecutive elements
     * arrive within the specified timeout. An element is emitted only when the gap
     * to the next element exceeds the timeout, or at stream end.
     *
     * <p>RxJava: {@code observable.debounce(timeout, unit)}</p>
     * <p>Reactor: {@code flux.debounce(Duration)}</p>
     * <p>Example: timestamps [0,50,80,200,230,300] with timeout=100 →
     *   elements at 80, 230, 300</p>
     *
     * @param timeout            maximum gap between consecutive elements in same bucket
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer that emits only the last element per debounce period
     */
    public static <T> Gatherer<T, ?, T> debounce(long timeout,
            ToLongFunction<? super T> timestampExtractor) {
        if (timeout < 0) throw new IllegalArgumentException("timeout must be >= 0, got " + timeout);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    T pending;
                    long pendingTs = Long.MIN_VALUE;
                    boolean hasPending = false;
                },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    if (state.hasPending && (ts - state.pendingTs) > timeout) {
                        if (!downstream.push(state.pending)) {
                            return false;
                        }
                    }
                    state.pending = element;
                    state.pendingTs = ts;
                    state.hasPending = true;
                    return true;
                },
                (state, downstream) -> {
                    if (state.hasPending && !downstream.push(state.pending)) {
                        return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ThrottleFirst (RxJava: throttleFirst, Reactor: sampleFirst)
    // ═══════════════════════════════════════════════

    /**
     * Emit the first element in each time window of the specified duration,
     * then suppress subsequent elements until the window expires.
     *
     * <p>RxJava: {@code observable.throttleFirst(windowSize, unit)}</p>
     * <p>Reactor: {@code flux.sampleFirst(Duration)}</p>
     * <p>Example: timestamps [0,10,50,100,110,150] with window=100 →
     *   elements at 0, 100</p>
     *
     * @param windowSize         time window duration
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer that emits at most one element per time window
     */
    public static <T> Gatherer<T, ?, T> throttleFirst(long windowSize,
            ToLongFunction<? super T> timestampExtractor) {
        if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be > 0, got " + windowSize);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    boolean firstWindowSeen = false;
                    long windowStart = 0L;
                },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    long currentWindowStart = Math.floorDiv(ts, windowSize) * windowSize;
                    if (!state.firstWindowSeen || currentWindowStart != state.windowStart) {
                        state.firstWindowSeen = true;
                        state.windowStart = currentWindowStart;
                        return downstream.push(element);
                    }
                    return true; // suppress
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ThrottleLast / Sample (RxJava: throttleLast/sample, Reactor: sample)
    // ═══════════════════════════════════════════════

    /**
     * Emit the last element in each time window of the specified duration.
     *
     * <p>Timestamps are bucketed with {@link Math#floorDiv(long, long)} so that
     * negative and slightly out-of-order timestamps are handled correctly.
     * When a timestamp advances past the end of a previously seen bucket, all
     * complete buckets are emitted in order.</p>
     *
     * <p>RxJava: {@code observable.sample(windowSize, unit)} / {@code throttleLast}</p>
     * <p>Reactor: {@code flux.sample(Duration)}</p>
     * <p>Example: timestamps [0,10,50,100,110,150] with window=100 →
     *   elements at 50, 150</p>
     *
     * @param windowSize         time window duration
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer that emits the last element per time window
     */
    public static <T> Gatherer<T, ?, T> throttleLast(long windowSize,
            ToLongFunction<? super T> timestampExtractor) {
        if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be > 0, got " + windowSize);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final NavigableMap<Long, T> buckets = new TreeMap<>();
                },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    long bucket = Math.floorDiv(ts, windowSize) * windowSize;
                    var it = state.buckets.entrySet().iterator();
                    while (it.hasNext()) {
                        var entry = it.next();
                        if (entry.getKey() + windowSize <= ts) {
                            if (!downstream.push(entry.getValue())) {
                                state.buckets.clear();
                                return false;
                            }
                            it.remove();
                        } else {
                            break;
                        }
                    }
                    state.buckets.put(bucket, element);
                    return true;
                },
                (state, downstream) -> {
                    for (T value : state.buckets.values()) {
                        if (!downstream.push(value)) {
                            return;
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Buffer (time-based) (RxJava: buffer(timespan), Reactor: buffer(Duration))
    // ═══════════════════════════════════════════════

    /**
     * Buffer elements into time-bucketed lists. Each bucket covers a fixed
     * time span, and elements are grouped by their timestamp's bucket.
     *
     * <p>Timestamps are bucketed with {@link Math#floorDiv(long, long)} so that
     * negative and slightly out-of-order timestamps are handled correctly. When a
     * timestamp advances past the end of a previously seen bucket, all complete
     * buckets are emitted in order.</p>
     *
     * <p>RxJava: {@code observable.buffer(timespan, unit)}</p>
     * <p>Reactor: {@code flux.buffer(Duration)}</p>
     *
     * @param timespan           time span per buffer bucket
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer emitting Lists of elements per time bucket
     */
    public static <T> Gatherer<T, ?, List<T>> bufferTime(long timespan,
            ToLongFunction<? super T> timestampExtractor) {
        if (timespan <= 0) throw new IllegalArgumentException("timespan must be > 0, got " + timespan);
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final NavigableMap<Long, List<T>> buckets = new TreeMap<>();
                },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    long bucket = Math.floorDiv(ts, timespan) * timespan;
                    var it = state.buckets.entrySet().iterator();
                    while (it.hasNext()) {
                        var entry = it.next();
                        if (entry.getKey() + timespan <= ts) {
                            if (!downstream.push(Collections.unmodifiableList(new ArrayList<>(entry.getValue())))) {
                                state.buckets.clear();
                                return false;
                            }
                            it.remove();
                        } else {
                            break;
                        }
                    }
                    state.buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(element);
                    return true;
                },
                (state, downstream) -> {
                    for (List<T> bucket : state.buckets.values()) {
                        if (!downstream.push(Collections.unmodifiableList(new ArrayList<>(bucket)))) {
                            return;
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Timestamp (RxJava: timestamp, Reactor: timestamp)
    // ═══════════════════════════════════════════════

    /**
     * Attach a timestamp to each element using the provided clock.
     *
     * <p>RxJava: {@code observable.timestamp()}</p>
     * <p>Reactor: {@code flux.timestamp()}</p>
     *
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer emitting {@link Timestamped} elements
     */
    public static <T> Gatherer<T, ?, Timestamped<T>> timestamp(
            ToLongFunction<? super T> timestampExtractor) {
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                (_, element, downstream) ->
                        downstream.push(new Timestamped<>(timestampExtractor.applyAsLong(element), element))
        );
    }

    // ═══════════════════════════════════════════════
    //  TimeInterval (Reactor: elapsed, RxJava: timeInterval)
    // ═══════════════════════════════════════════════

    /**
     * Measure the time interval between consecutive elements.
     * The first element has an elapsed time of 0.
     *
     * <p>RxJava: {@code observable.timeInterval()}</p>
     * <p>Reactor: {@code flux.elapsed()}</p>
     *
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer emitting {@link Timed} elements with elapsed time
     */
    public static <T> Gatherer<T, ?, Timed<T>> timeInterval(
            ToLongFunction<? super T> timestampExtractor) {
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() { long lastTs = Long.MIN_VALUE; },
                (state, element, downstream) -> {
                    long ts = timestampExtractor.applyAsLong(element);
                    long elapsed = state.lastTs == Long.MIN_VALUE ? 0 : ts - state.lastTs;
                    state.lastTs = ts;
                    return downstream.push(new Timed<>(elapsed, element));
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  DoOnNext / DoOnComplete / DoOnError / DoFinally
    // ═══════════════════════════════════════════════

    /**
     * Invoke a side-effect on each element, then pass it through.
     * Unlike {@link Stream#peek}, this is named after RxJava/Reactor's convention.
     *
     * <p>RxJava: {@code observable.doOnNext(consumer)}</p>
     * <p>Reactor: {@code flux.doOnNext(consumer)}</p>
     *
     * @param action  side-effect to perform on each element
     * @return a Gatherer that invokes the action and passes elements through
     */
    public static <T> Gatherer<T, ?, T> doOnNext(Consumer<? super T> action) {
        Objects.requireNonNull(action);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    action.accept(element);
                    return downstream.push(element);
                }
        );
    }

    /**
     * Invoke a side-effect when the stream completes (finisher).
     *
     * <p>RxJava: {@code observable.doOnComplete(action)}</p>
     * <p>Reactor: {@code flux.doOnComplete(action)}</p>
     *
     * @param action  side-effect to perform on stream completion
     * @return a Gatherer that invokes the action when the stream ends
     */
    public static <T> Gatherer<T, ?, T> doOnComplete(Runnable action) {
        Objects.requireNonNull(action);
        return Gatherer.ofSequential(
                (_, element, downstream) -> downstream.push(element),
                (_, downstream) -> action.run()
        );
    }

    /**
     * Invoke a side-effect if {@code downstream.push(...)} throws an exception.
     * Catches unchecked exceptions ({@link RuntimeException} / {@link Error})
     * thrown while pushing an element downstream, calls the error handler, then
     * re-throws to propagate the error.
     *
     * <p>RxJava: {@code observable.doOnError(consumer)}</p>
     * <p>Reactor: {@code flux.doOnError(consumer)}</p>
     *
     * <p><em>Note:</em> Java Streams do not have an error channel. This operator
     * only observes errors thrown by {@code downstream.push(...)} (for example,
     * from downstream operators), not by upstream gatherers or the source.</p>
     *
     * @param errorHandler  side-effect to perform on error
     * @return a Gatherer that observes errors and re-throws them
     */
    public static <T> Gatherer<T, ?, T> doOnError(Consumer<? super Throwable> errorHandler) {
        Objects.requireNonNull(errorHandler);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    try {
                        return downstream.push(element);
                    } catch (RuntimeException | Error e) {
                        errorHandler.accept(e);
                        throw e;
                    }
                }
        );
    }

    /**
     * Invoke a side-effect when the stream terminates, regardless of
     * normal completion or error. The action receives the notification type.
     *
     * <p>RxJava: {@code observable.doFinally(action)}</p>
     * <p>Reactor: {@code flux.doFinally(action)}</p>
     *
     * @param action  side-effect to perform on termination, receives "complete" or "error"
     * @return a Gatherer that invokes the action on any termination
     */
    public static <T> Gatherer<T, ?, T> doFinally(Consumer<String> action) {
        Objects.requireNonNull(action);
        return Gatherer.ofSequential(
                (_, element, downstream) -> downstream.push(element),
                (_, downstream) -> action.accept("complete")
                // Note: error-path doFinally cannot be captured in Gatherer
                // since errors propagate as exceptions, not through the Gatherer API
        );
    }

    // ═══════════════════════════════════════════════
    //  Materialize / Dematerialize
    // ═══════════════════════════════════════════════

    /**
     * Convert a stream into a stream of {@link Notification} objects.
     * Each element becomes {@link Notification.OnNext}, and the stream end
     * becomes {@link Notification.OnComplete}.
     *
     * <p>RxJava: {@code observable.materialize()}</p>
     * <p>Reactor: {@code flux.materialize()}</p>
     */
    public static <T> Gatherer<T, ?, Notification<T>> materialize() {
        return Gatherer.ofSequential(
                (_, element, downstream) ->
                        downstream.push(new Notification.OnNext<>(element)),
                (_, downstream) ->
                        downstream.push(new Notification.OnComplete<T>())
        );
    }

    /**
     * Convert a stream of {@link Notification} objects back into a regular stream.
     * OnNext emissions are pushed downstream; OnComplete is consumed;
     * OnError is thrown as an exception.
     *
     * <p>RxJava: {@code observable.dematerialize()}</p>
     * <p>Reactor: {@code flux.dematerialize()}</p>
     */
    public static <T> Gatherer<Notification<T>, ?, T> dematerialize() {
        return Gatherer.ofSequential(
                (_, notification, downstream) -> {
                    switch (notification) {
                        case Notification.OnNext<T> onNext -> {
                            return downstream.push(onNext.value());
                        }
                        case Notification.OnError<T> onError -> {
                            Throwable t = onError.error();
                            if (t instanceof RuntimeException re) throw re;
                            if (t instanceof Error er) throw er;
                            throw new RuntimeException(t);
                        }
                        case Notification.OnComplete<T> ignored -> {} // end of stream
                    }
                    return true;
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  OnErrorReturn (RxJava: onErrorReturn, Reactor: onErrorReturn)
    // ═══════════════════════════════════════════════

    /**
     * If a mapping function throws a {@link RuntimeException} or {@link Error},
     * emit a fallback value instead. This wraps a map operation with error handling.
     *
     * <p>RxJava: {@code observable.onErrorReturn(fallback)}</p>
     * <p>Reactor: {@code flux.onErrorReturn(fallback)}</p>
     *
     * @param mapper     mapping function that may throw
     * @param fallback   value to emit if mapper throws
     * @return a Gatherer that maps elements, falling back on error
     */
    public static <T, R> Gatherer<T, ?, R> onErrorReturn(
            Function<? super T, ? extends R> mapper,
            R fallback) {
        Objects.requireNonNull(mapper);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    try {
                        return downstream.push(mapper.apply(element));
                    } catch (RuntimeException | Error e) {
                        return downstream.push(fallback);
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  OnErrorResume (RxJava: onErrorResumeNext, Reactor: onErrorResume)
    // ═══════════════════════════════════════════════

    /**
     * If a mapping function throws a {@link RuntimeException} or {@link Error},
     * switch to a fallback iterable for that element. The fallback factory is
     * invoked with the caught {@link RuntimeException}; {@link Error}s are
     * re-thrown without invoking the factory.
     *
     * <p>RxJava: {@code observable.onErrorResumeNext(fallbackFactory)}</p>
     * <p>Reactor: {@code flux.onErrorResume(fallbackFunction)}</p>
     *
     * @param mapper          mapping function that may throw
     * @param fallbackFactory function producing fallback iterable given the error
     * @return a Gatherer that maps elements, resuming with fallback on error
     */
    public static <T, R> Gatherer<T, ?, R> onErrorResume(
            Function<? super T, ? extends R> mapper,
            Function<Exception, ? extends Iterable<? extends R>> fallbackFactory) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(fallbackFactory);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    try {
                        return downstream.push(mapper.apply(element));
                    } catch (RuntimeException | Error e) {
                        Iterable<? extends R> fallback;
                        if (e instanceof RuntimeException re) {
                            fallback = Objects.requireNonNull(fallbackFactory.apply(re),
                                    "fallbackFactory returned null");
                        } else {
                            throw e;
                        }
                        for (R r : fallback) {
                            if (!downstream.push(r)) return false;
                        }
                        return true;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Retry (RxJava: retry, Reactor: retry)
    // ═══════════════════════════════════════════════

    /**
     * Retry a mapping operation up to a maximum number of attempts when it throws.
     * Each retry re-applies the mapper to the same input element.
     *
     * <p>RxJava: {@code observable.retry(maxAttempts)}</p>
     * <p>Reactor: {@code flux.retry(maxAttempts)}</p>
     *
     * <p><em>Limitation:</em> Only retries individual element mapping, not the
     * entire stream. True stream-level retry requires a re-subscribable source,
     * which Java Streams do not support.</p>
     *
     * @param mapper      mapping function that may throw
     * @param maxRetries  maximum number of retries (0 = no retry)
     * @return a Gatherer that retries the mapper on failure
     */
    public static <T, R> Gatherer<T, ?, R> retry(
            Function<? super T, ? extends R> mapper,
            int maxRetries) {
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
        Objects.requireNonNull(mapper);
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    Throwable lastException = null;
                    for (int attempt = 0; attempt <= maxRetries; attempt++) {
                        try {
                            return downstream.push(mapper.apply(element));
                        } catch (RuntimeException | Error e) {
                            lastException = e;
                        }
                    }
                    if (lastException instanceof RuntimeException re) throw re;
                    if (lastException instanceof Error er) throw er;
                    throw new RuntimeException("Retry exhausted after " + maxRetries + " retries", lastException);
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Repeat (RxJava: repeat, Reactor: repeat)
    // ═══════════════════════════════════════════════

    /**
     * Repeat the stream's elements N times.
     *
     * <p>RxJava: {@code observable.repeat(n)}</p>
     * <p>Reactor: {@code flux.repeat(n)}</p>
     * <p>Example: {@code [1,2,3].repeat(2) → [1,2,3,1,2,3]}</p>
     *
     * @param times  number of times to repeat (1 = no extra repetition)
     * @return a Gatherer that repeats the buffered elements
     */
    public static <T> Gatherer<T, ?, T> repeat(int times) {
        if (times < 1) throw new IllegalArgumentException("times must be >= 1, got " + times);
        return Gatherer.ofSequential(
                () -> new Object() { final List<T> buffer = new ArrayList<>(); },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    return true;
                },
                (state, downstream) -> {
                    for (int i = 0; i < times; i++) {
                        for (T e : state.buffer) {
                            if (!downstream.push(e)) {
                                return;
                            }
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  DefaultIfEmpty (RxJava: defaultIfEmpty, Reactor: defaultIfEmpty)
    // ═══════════════════════════════════════════════

    /**
     * Emit a default value if the stream is empty.
     *
     * <p>RxJava: {@code observable.defaultIfEmpty(value)}</p>
     * <p>Reactor: {@code flux.defaultIfEmpty(value)}</p>
     *
     * @param defaultValue  value to emit if stream is empty
     * @return a Gatherer that emits the default value for empty streams
     */
    public static <T> Gatherer<T, ?, T> defaultIfEmpty(T defaultValue) {
        return Gatherer.ofSequential(
                () -> new Object() { boolean hasElement = false; },
                (state, element, downstream) -> {
                    state.hasElement = true;
                    return downstream.push(element);
                },
                (state, downstream) -> {
                    if (!state.hasElement) {
                        downstream.push(defaultValue);
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  SwitchIfEmpty (RxJava: switchIfEmpty, Reactor: switchIfEmpty)
    // ═══════════════════════════════════════════════

    /**
     * Switch to a fallback iterable if the stream is empty.
     *
     * <p>RxJava: {@code observable.switchIfEmpty(fallbackObservable)}</p>
     * <p>Reactor: {@code flux.switchIfEmpty(fallbackFlux)}</p>
     *
     * @param fallback  iterable to use when stream is empty
     * @return a Gatherer that switches to fallback for empty streams
     */
    public static <T> Gatherer<T, ?, T> switchIfEmpty(Iterable<? extends T> fallback) {
        Objects.requireNonNull(fallback);
        return Gatherer.ofSequential(
                () -> new Object() { boolean hasElement = false; },
                (state, element, downstream) -> {
                    state.hasElement = true;
                    return downstream.push(element);
                },
                (state, downstream) -> {
                    if (!state.hasElement) {
                        for (T t : fallback) {
                            if (!downstream.push(t)) return;
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  StartWith (RxJava: startWith, Reactor: startWith)
    // ═══════════════════════════════════════════════

    /**
     * Prepend elements from an iterable before the stream.
     *
     * <p>RxJava: {@code observable.startWith(iterable)}</p>
     * <p>Reactor: {@code flux.startWith(iterable)}</p>
     *
     * @param prefix  iterable to prepend
     * @return a Gatherer that emits prefix elements first, then the stream
     */
    public static <T> Gatherer<T, ?, T> startWith(Iterable<? extends T> prefix) {
        Objects.requireNonNull(prefix);
        return Gatherer.ofSequential(
                () -> new Object() { boolean emittedPrefix = false; },
                (state, element, downstream) -> {
                    if (!state.emittedPrefix) {
                        for (T t : prefix) {
                            if (!downstream.push(t)) {
                                state.emittedPrefix = true;
                                return false;
                            }
                        }
                        state.emittedPrefix = true;
                    }
                    return downstream.push(element);
                },
                (state, downstream) -> {
                    if (!state.emittedPrefix) {
                        for (T t : prefix) {
                            if (!downstream.push(t)) return;
                        }
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ConcatWith (RxJava: concat, Reactor: concatWith)
    // ═══════════════════════════════════════════════

    /**
     * Append elements from an iterable after the stream.
     *
     * <p>RxJava: {@code observable.concatWith(other)}</p>
     * <p>Reactor: {@code flux.concatWith(other)}</p>
     *
     * @param suffix  iterable to append
     * @return a Gatherer that emits the stream, then the suffix
     */
    public static <T> Gatherer<T, ?, T> concatWith(Iterable<? extends T> suffix) {
        Objects.requireNonNull(suffix);
        return Gatherer.ofSequential(
                (_, element, downstream) -> downstream.push(element),
                (_, downstream) -> {
                    for (T t : suffix) {
                        if (!downstream.push(t)) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Delay (Reactor: delayElements, RxJava: delay)
    // ═══════════════════════════════════════════════

    /**
     * Reorder elements by their timestamp, simulating a delay.
     * Since Java Streams are synchronous and pull-based, this cannot perform
     * wall-clock delays. Instead, it sorts elements by timestamp.
     *
     * <p>Reactor: {@code flux.delayElements(Duration)}</p>
     * <p><em>Limitation:</em> This is a <strong>sort by timestamp</strong>,
     * not a true wall-clock delay. Elements are buffered and emitted in
     * timestamp order at the finisher.</p>
     *
     * @param timestampExtractor  function to extract timestamp from element
     * @return a Gatherer that reorders elements by timestamp
     */
    public static <T> Gatherer<T, ?, T> delay(ToLongFunction<? super T> timestampExtractor) {
        Objects.requireNonNull(timestampExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final List<T> buffer = new ArrayList<>();
                },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    return true;
                },
                (state, downstream) -> {
                    state.buffer.sort(Comparator.comparingLong(timestampExtractor::applyAsLong));
                    for (T e : state.buffer) {
                        if (!downstream.push(e)) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ElementAt (RxJava: elementAt, Reactor: elementAt)
    // ═══════════════════════════════════════════════

    /**
     * Emit only the element at the specified index, or empty if out of bounds.
     *
     * <p>RxJava: {@code observable.elementAt(index)}</p>
     * <p>Reactor: {@code flux.elementAt(index)}</p>
     *
     * @param index  zero-based index of the element to emit
     * @return a Gatherer that emits at most one element at the given index
     */
    public static <T> Gatherer<T, ?, Optional<T>> elementAt(long index) {
        if (index < 0) throw new IllegalArgumentException("index must be >= 0, got " + index);
        return Gatherer.ofSequential(
                () -> new Object() { long currentIndex = 0; },
                (state, element, downstream) -> {
                    if (state.currentIndex == index) {
                        downstream.push(Optional.ofNullable(element));
                        return false; // short-circuit
                    }
                    state.currentIndex++;
                    return true;
                },
                (state, downstream) -> {
                    downstream.push(Optional.empty());
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  First / FirstOr (RxJava: first, Reactor: next)
    // ═══════════════════════════════════════════════

    /**
     * Emit only the first element, or empty if the stream is empty.
     *
     * <p>RxJava: {@code observable.first()}</p>
     * <p>Reactor: {@code flux.next()}</p>
     *
     * @return a Gatherer that emits at most the first element
     */
    public static <T> Gatherer<T, ?, Optional<T>> first() {
        return Gatherer.ofSequential(
                (_, element, downstream) -> {
                    downstream.push(Optional.ofNullable(element));
                    return false; // short-circuit after first
                },
                (_, downstream) -> {
                    downstream.push(Optional.empty());
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  Last (RxJava: last, Reactor: last)
    // ═══════════════════════════════════════════════

    /**
     * Emit only the last element, or empty if the stream is empty.
     *
     * <p>RxJava: {@code observable.last()}</p>
     * <p>Reactor: {@code flux.last()}</p>
     *
     * @return a Gatherer that emits at most the last element
     */
    public static <T> Gatherer<T, ?, Optional<T>> last() {
        return Gatherer.ofSequential(
                () -> new Object() { T lastElement; boolean hasElement = false; },
                (state, element, downstream) -> {
                    state.lastElement = element;
                    state.hasElement = true;
                    return true;
                },
                (state, downstream) -> {
                    if (state.hasElement) {
                        downstream.push(Optional.ofNullable(state.lastElement));
                    } else {
                        downstream.push(Optional.empty());
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  SkipLast / TakeLast (RxJava: skipLast, takeLast)
    // ═══════════════════════════════════════════════

    /**
     * Skip the last N elements of the stream.
     *
     * <p>RxJava: {@code observable.skipLast(n)}</p>
     * <p>Reactor: {@code flux.skipLast(n)}</p>
     *
     * @param n  number of elements to skip from the end
     * @return a Gatherer that skips the last N elements
     */
    public static <T> Gatherer<T, ?, T> skipLast(int n) {
        if (n < 0) throw new IllegalArgumentException("n must be >= 0, got " + n);
        if (n == 0) return Gatherer.ofSequential((_, element, downstream) -> downstream.push(element));
        return Gatherer.ofSequential(
                () -> new Object() { final LinkedList<T> queue = new LinkedList<>(); },
                (state, element, downstream) -> {
                    state.queue.addLast(element);
                    if (state.queue.size() > n) {
                        return downstream.push(state.queue.removeFirst());
                    }
                    return true;
                }
        );
    }

    /**
     * Take only the last N elements of the stream.
     *
     * <p>RxJava: {@code observable.takeLast(n)}</p>
     * <p>Reactor: {@code flux.takeLast(n)}</p>
     *
     * @param n  number of elements to take from the end
     * @return a Gatherer that emits only the last N elements
     */
    public static <T> Gatherer<T, ?, T> takeLast(int n) {
        if (n < 0) throw new IllegalArgumentException("n must be >= 0, got " + n);
        if (n == 0) return Gatherer.ofSequential((_, element, downstream) -> true);
        return Gatherer.ofSequential(
                () -> new Object() { final LinkedList<T> queue = new LinkedList<>(); },
                (state, element, downstream) -> {
                    state.queue.addLast(element);
                    if (state.queue.size() > n) {
                        state.queue.removeFirst();
                    }
                    return true;
                },
                (state, downstream) -> {
                    for (T e : state.queue) {
                        if (!downstream.push(e)) return;
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  DistinctUntilChanged (RxJava: distinctUntilChanged, Reactor: distinctUntilChanged)
    // ═══════════════════════════════════════════════

    /**
     * Suppress consecutive duplicate elements. Unlike {@link Stream#distinct}
     * which removes all duplicates globally, this only removes adjacent duplicates.
     *
     * <p>RxJava: {@code observable.distinctUntilChanged()}</p>
     * <p>Reactor: {@code flux.distinctUntilChanged()}</p>
     * <p>Example: {@code [1,1,2,2,3,1,1].distinctUntilChanged() → [1,2,3,1]}</p>
     *
     * @return a Gatherer that suppresses consecutive duplicates
     */
    public static <T> Gatherer<T, ?, T> distinctUntilChanged() {
        return Gatherer.ofSequential(
                () -> new Object() {
                    Object previous = new Object(); // sentinel
                    boolean hasPrevious = false;
                },
                (state, element, downstream) -> {
                    if (state.hasPrevious && Objects.equals(state.previous, element)) {
                        return true; // suppress
                    }
                    state.previous = element;
                    state.hasPrevious = true;
                    return downstream.push(element);
                }
        );
    }

    /**
     * Suppress consecutive duplicates by key selector.
     *
     * <p>RxJava: {@code observable.distinctUntilChanged(keySelector)}</p>
     * <p>Reactor: {@code flux.distinctUntilChanged(keySelector)}</p>
     *
     * @param keyExtractor  function to compute the comparison key
     * @return a Gatherer that suppresses consecutive duplicates by key
     */
    public static <T, K> Gatherer<T, ?, T> distinctUntilChanged(
            Function<? super T, ? extends K> keyExtractor) {
        Objects.requireNonNull(keyExtractor);
        return Gatherer.ofSequential(
                () -> new Object() {
                    Object previousKey = new Object(); // sentinel
                    boolean hasPrevious = false;
                },
                (state, element, downstream) -> {
                    K key = keyExtractor.apply(element);
                    if (state.hasPrevious && Objects.equals(state.previousKey, key)) {
                        return true; // suppress
                    }
                    state.previousKey = key;
                    state.hasPrevious = true;
                    return downstream.push(element);
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  WithLatestFrom (RxJava: withLatestFrom, Reactor: withLatestFrom)
    // ═══════════════════════════════════════════════

    /**
     * Combine each stream element with the latest value from another iterable.
     * If the other iterable has not yet produced a value, the element is skipped.
     *
     * <p>RxJava: {@code observable.withLatestFrom(other, combiner)}</p>
     * <p>Reactor: {@code flux.withLatestFrom(other, combiner)}</p>
     *
     * @param other    the other iterable providing "latest" values
     * @param combiner function combining main element with latest other value
     * @return a Gatherer that combines each element with the latest other value
     */
    public static <T, U, R> Gatherer<T, ?, R> withLatestFrom(
            Iterable<? extends U> other,
            BiFunction<? super T, ? super U, ? extends R> combiner) {
        Objects.requireNonNull(other);
        Objects.requireNonNull(combiner);
        return Gatherer.ofSequential(
                () -> new Object() {
                    final Iterator<? extends U> otherIter = other.iterator();
                    U latestOther = null;
                    boolean hasLatest = false;
                },
                (state, element, downstream) -> {
                    if (state.otherIter.hasNext()) {
                        state.latestOther = state.otherIter.next();
                        state.hasLatest = true;
                    }
                    if (state.hasLatest) {
                        return downstream.push(combiner.apply(element, state.latestOther));
                    }
                    return true; // skip until other has a value
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ScanWithSeed (RxJava: scan, Reactor: scan)
    // ═══════════════════════════════════════════════

    /**
     * Scan with seed, emitting each intermediate result (including the seed).
     * This is the same as {@link SequenceGatherers#scanLeft} but included here
     * for RxJava/Reactor naming consistency.
     *
     * <p>RxJava: {@code observable.scan(seed, accumulator)}</p>
     * <p>Reactor: {@code flux.scan(seed, accumulator)}</p>
     */
    public static <T, A> Gatherer<T, ?, A> scan(A seed, BiFunction<A, T, A> accumulator) {
        Objects.requireNonNull(accumulator);
        return Gatherer.ofSequential(
                () -> new Object() { A acc = seed; boolean emittedSeed = false; },
                (state, element, downstream) -> {
                    if (!state.emittedSeed) {
                        if (!downstream.push(state.acc)) {
                            state.emittedSeed = true;
                            return false;
                        }
                        state.emittedSeed = true;
                    }
                    state.acc = accumulator.apply(state.acc, element);
                    return downstream.push(state.acc);
                },
                (state, downstream) -> {
                    if (!state.emittedSeed) {
                        downstream.push(state.acc);
                    }
                }
        );
    }

    // ═══════════════════════════════════════════════
    //  ReduceWith (Reactor: reduce, RxJava: reduce)
    // ═══════════════════════════════════════════════

    /**
     * Reduce with an explicit seed and accumulator, emitting a single result.
     * Unlike {@link Stream#reduce}, this always emits a value (the seed for empty streams).
     *
     * <p>RxJava: {@code observable.reduce(seed, accumulator)}</p>
     * <p>Reactor: {@code flux.reduce(seed, accumulator)}</p>
     *
     * @param seed        initial accumulator
     * @param accumulator accumulator function
     * @return a Gatherer that emits a single reduced result
     */
    public static <T, A> Gatherer<T, ?, A> reduceWith(A seed, BiFunction<A, T, A> accumulator) {
        Objects.requireNonNull(accumulator);
        return Gatherer.ofSequential(
                () -> new Object() { A acc = seed; },
                (state, element, downstream) -> {
                    state.acc = accumulator.apply(state.acc, element);
                    return true;
                },
                (state, downstream) -> downstream.push(state.acc)
        );
    }

    // ═══════════════════════════════════════════════
    //  CollectList (Reactor: collectList)
    // ═══════════════════════════════════════════════

    /**
     * Collect all elements into a single List, emitted at the end.
     *
     * <p>Reactor: {@code flux.collectList()}</p>
     *
     * @return a Gatherer that emits one List containing all elements
     */
    public static <T> Gatherer<T, ?, List<T>> collectList() {
        return Gatherer.ofSequential(
                () -> new Object() { final List<T> buffer = new ArrayList<>(); },
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    return true;
                },
                (state, downstream) -> downstream.push(Collections.unmodifiableList(new ArrayList<>(state.buffer)))
        );
    }

    // ═══════════════════════════════════════════════
    //  MapWithIndex (Reactor: index, RxJava: zipWithIndex-like)
    // ═══════════════════════════════════════════════

    /**
     * Pair each element with its zero-based index, using a custom combiner.
     *
     * <p>Reactor: {@code flux.index()}</p>
     *
     * @param combiner  function combining index and element
     * @return a Gatherer emitting combined index+element results
     */
    public static <T, R> Gatherer<T, ?, R> mapWithIndex(BiFunction<Long, ? super T, ? extends R> combiner) {
        Objects.requireNonNull(combiner);
        return Gatherer.ofSequential(
                () -> new Object() { long index = 0; },
                (state, element, downstream) -> {
                    long idx = state.index++;
                    return downstream.push(combiner.apply(idx, element));
                }
        );
    }
}
