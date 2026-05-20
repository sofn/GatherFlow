package com.lesofn.gatherflow;

import com.lesofn.gatherflow.window.*;
import com.lesofn.gatherflow.sequence.SequenceGatherers;
import com.lesofn.gatherflow.reactive.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.stream.*;

import static com.lesofn.gatherflow.window.WindowGatherers.*;
import static com.lesofn.gatherflow.sequence.SequenceGatherers.*;
import static com.lesofn.gatherflow.reactive.ReactiveGatherers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Streaming-mode tests: data arrives at irregular intervals via a BlockingQueue-backed stream.
 * Verifies that operators emit results incrementally (in the integrator) rather than
 * only at stream end (in the finisher).
 */
@DisplayName("Streaming Operator Tests")
class StreamingOperatorTest {

    record TimedEvent(long ts, String value) {}

    // ─────────────────────────────────────────────
    //  Helper: create a stream from a BlockingQueue
    // ─────────────────────────────────────────────

    /**
     * Creates a Stream that reads from a BlockingQueue.
     * {@code Optional.empty()} signals end-of-stream.
     */
    static <T> Stream<T> streamFromQueue(BlockingQueue<Optional<T>> queue) {
        Spliterator<T> spliterator = new Spliterator<T>() {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    Optional<T> item = queue.take();
                    if (item.isEmpty()) return false;
                    action.accept(item.get());
                    return true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public Spliterator<T> trySplit() { return null; }

            @Override
            public long estimateSize() { return Long.MAX_VALUE; }

            @Override
            public int characteristics() { return Spliterator.ORDERED | Spliterator.NONNULL; }
        };
        return StreamSupport.stream(spliterator, false);
    }

    /**
     * Feeds elements into the queue and signals end-of-stream.
     */
    @SafeVarargs
    static <T> void feedAndClose(BlockingQueue<Optional<T>> queue, T... elements) {
        try {
            for (T e : elements) {
                queue.put(Optional.of(e));
            }
            queue.put(Optional.empty());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ═══════════════════════════════════════════════
    //  Operators that SHOULD emit incrementally
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("throttleLast — incremental emission")
    class ThrottleLastStreamingTest {

        @Test
        @DisplayName("emits previous bucket when new bucket starts")
        void emitsIncrementally() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<TimedEvent> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(throttleLast(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed bucket 0
            queue.put(Optional.of(new TimedEvent(0, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            Thread.sleep(50);
            // No results yet — bucket 0 not complete
            assertEquals(0, results.size());

            // Feed bucket 1 — should trigger emission of bucket 0
            queue.put(Optional.of(new TimedEvent(100, "c")));
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(new TimedEvent(50, "b"), results.get(0)); // last of bucket 0

            // End stream — finisher emits bucket 1
            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(List.of(new TimedEvent(50, "b"), new TimedEvent(100, "c")),
                    new ArrayList<>(results));
        }
    }

    @Nested
    @DisplayName("bufferTime — incremental emission")
    class BufferTimeStreamingTest {

        @Test
        @DisplayName("emits previous bucket when new bucket starts")
        void emitsIncrementally() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<List<TimedEvent>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(bufferTime(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed bucket 0
            queue.put(Optional.of(new TimedEvent(10, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            Thread.sleep(50);
            assertEquals(0, results.size()); // bucket 0 not complete

            // Feed bucket 1 — should trigger emission of bucket 0
            queue.put(Optional.of(new TimedEvent(110, "c")));
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(List.of(new TimedEvent(10, "a"), new TimedEvent(50, "b")), results.get(0));

            // End stream
            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(2, results.size());
            assertEquals(List.of(new TimedEvent(110, "c")), results.get(1));
        }
    }

    @Nested
    @DisplayName("tumblingTimeWindow — incremental emission")
    class TumblingTimeWindowStreamingTest {

        @Test
        @DisplayName("emits previous window when new window starts")
        void emitsIncrementally() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<Window<TimedEvent>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(tumblingTimeWindow(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed window [0-99]
            queue.put(Optional.of(new TimedEvent(10, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            Thread.sleep(50);
            assertEquals(0, results.size()); // window not complete

            // Feed window [100-199] — should trigger emission of window [0-99]
            queue.put(Optional.of(new TimedEvent(110, "c")));
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(0, results.get(0).startIndex());
            assertEquals(99, results.get(0).endIndex());
            assertEquals(2, results.get(0).size());

            // End stream
            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(2, results.size());
            assertEquals(100, results.get(1).startIndex());
        }
    }

    @Nested
    @DisplayName("debounce — incremental emission (gap-triggered)")
    class DebounceStreamingTest {

        @Test
        @DisplayName("emits pending element when gap exceeds timeout")
        void emitsOnGap() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<TimedEvent> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(debounce(50, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed elements within timeout
            queue.put(Optional.of(new TimedEvent(0, "a")));
            queue.put(Optional.of(new TimedEvent(30, "b"))); // gap=30 < 50, not emitted
            Thread.sleep(50);
            assertEquals(0, results.size()); // no gap exceeded yet

            // Feed element with gap > timeout — should emit "b" (pending)
            queue.put(Optional.of(new TimedEvent(100, "c"))); // gap=70 > 50
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(new TimedEvent(30, "b"), results.get(0));

            // End stream — finisher emits "c"
            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(List.of(new TimedEvent(30, "b"), new TimedEvent(100, "c")),
                    new ArrayList<>(results));
        }
    }

    @Nested
    @DisplayName("sessionWindow — incremental emission (gap-triggered)")
    class SessionWindowStreamingTest {

        @Test
        @DisplayName("emits session when gap exceeds threshold")
        void emitsOnGap() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<Window<TimedEvent>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(sessionWindow(5, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed elements within session
            queue.put(Optional.of(new TimedEvent(1, "a")));
            queue.put(Optional.of(new TimedEvent(3, "b")));
            Thread.sleep(50);
            assertEquals(0, results.size()); // session not closed

            // Feed element with gap > 5 — should close session 0
            queue.put(Optional.of(new TimedEvent(20, "c")));
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(2, results.get(0).size());

            // End stream — finisher emits session 1
            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(2, results.size());
        }
    }

    // ═══════════════════════════════════════════════
    //  Operators that ONLY emit in finisher (not streaming-safe)
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("FINISHER_ONLY operators — no intermediate results")
    class FinisherOnlyTest {

        @Test
        @DisplayName("foldLeft produces no results until stream ends")
        void foldLeftNoIntermediate() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<Integer> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(foldLeft(0, Integer::sum))
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of(1));
            queue.put(Optional.of(2));
            queue.put(Optional.of(3));
            Thread.sleep(100);
            // No results yet — foldLeft only emits in finisher
            assertEquals(0, results.size());

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(List.of(6), new ArrayList<>(results));
        }

        @Test
        @DisplayName("reverse produces no results until stream ends")
        void reverseNoIntermediate() throws Exception {
            BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();
            List<String> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(reverse())
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of("a"));
            queue.put(Optional.of("b"));
            Thread.sleep(100);
            assertEquals(0, results.size()); // no intermediate results

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(List.of("b", "a"), new ArrayList<>(results));
        }

        @Test
        @DisplayName("last produces no results until stream ends")
        void lastNoIntermediate() throws Exception {
            BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();
            List<Optional<String>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(ReactiveGatherers.last())
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of("a"));
            queue.put(Optional.of("b"));
            Thread.sleep(100);
            assertEquals(0, results.size());

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(List.of(Optional.of("b")), new ArrayList<>(results));
        }

        @Test
        @DisplayName("slidingTimeWindow produces no results until stream ends")
        void slidingTimeWindowNoIntermediate() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<Window<TimedEvent>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of(new TimedEvent(0, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            queue.put(Optional.of(new TimedEvent(100, "c")));
            Thread.sleep(100);
            // slidingTimeWindow only emits in finisher — needs all data for overlapping windows
            assertEquals(0, results.size());

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertFalse(results.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Operators that emit incrementally (streaming-safe)
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("INTEGRATOR_ONLY operators — streaming-safe")
    class IntegratorOnlyTest {

        @Test
        @DisplayName("throttleFirst emits immediately per bucket")
        void throttleFirstStreaming() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<TimedEvent> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(throttleFirst(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // First element in bucket 0 — should emit immediately
            queue.put(Optional.of(new TimedEvent(0, "a")));
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(new TimedEvent(0, "a"), results.get(0));

            // Second element in bucket 0 — suppressed
            queue.put(Optional.of(new TimedEvent(50, "b")));
            Thread.sleep(50);
            assertEquals(1, results.size()); // still just "a"

            // First element in bucket 1 — should emit
            queue.put(Optional.of(new TimedEvent(100, "c")));
            Thread.sleep(50);
            assertEquals(2, results.size());

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
        }

        @Test
        @DisplayName("tumblingWindow emits when window fills")
        void tumblingWindowStreaming() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<Window<Integer>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(tumblingWindow(2))
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of(1));
            Thread.sleep(50);
            assertEquals(0, results.size()); // window not full

            queue.put(Optional.of(2));
            Thread.sleep(50);
            assertEquals(1, results.size()); // window full, emitted

            queue.put(Optional.of(3));
            Thread.sleep(50);
            assertEquals(1, results.size()); // next window not full

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(2, results.size()); // partial window emitted in finisher
        }

        @Test
        @DisplayName("grouped emits when group fills")
        void groupedStreaming() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<List<Integer>> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(grouped(2))
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of(1));
            Thread.sleep(50);
            assertEquals(0, results.size());

            queue.put(Optional.of(2));
            Thread.sleep(50);
            assertEquals(1, results.size());
            assertEquals(List.of(1, 2), results.get(0));

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(1, results.size()); // no partial group
        }

        @Test
        @DisplayName("scanLeft emits each step incrementally")
        void scanLeftStreaming() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<Integer> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue)
                    .gather(scanLeft(0, Integer::sum))
                    .forEach(results::add);
                done.countDown();
            });

            queue.put(Optional.of(1));
            Thread.sleep(50);
            // scanLeft emits initial value (0) then first step (0+1=1)
            assertEquals(2, results.size());
            assertEquals(0, results.get(0)); // initial value
            assertEquals(1, results.get(1)); // first step

            queue.put(Optional.of(2));
            Thread.sleep(50);
            assertEquals(3, results.size());
            assertEquals(3, results.get(2)); // 1+2=3

            queue.put(Optional.empty());
            done.await(5, TimeUnit.SECONDS);
            // No additional emission — initial was already emitted in integrator
            assertEquals(3, results.size());
        }
    }
}
