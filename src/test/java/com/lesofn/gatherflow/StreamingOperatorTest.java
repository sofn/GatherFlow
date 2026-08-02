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
     * <p>The optional {@code consumed} latch is counted down once per element
     * consumed by the stream, allowing tests to wait deterministically for
     * processing without arbitrary sleeps.</p>
     */
    static <T> Stream<T> streamFromQueue(BlockingQueue<Optional<T>> queue,
                                          AtomicReference<CountDownLatch> consumed) {
        Spliterator<T> spliterator = new Spliterator<>() {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    Optional<T> item = queue.take();
                    if (item.isEmpty()) return false;
                    action.accept(item.get());
                    CountDownLatch latch = consumed == null ? null : consumed.get();
                    if (latch != null) {
                        latch.countDown();
                    }
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

    static <T> Stream<T> streamFromQueue(BlockingQueue<Optional<T>> queue) {
        return streamFromQueue(queue, null);
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

    static void expectConsumed(AtomicReference<CountDownLatch> consumed, int count) {
        consumed.set(new CountDownLatch(count));
    }

    static void awaitConsumed(AtomicReference<CountDownLatch> consumed) throws InterruptedException {
        CountDownLatch latch = consumed.get();
        assertTrue(latch.await(2, TimeUnit.SECONDS), "expected elements to be consumed");
    }

    // ═══════════════════════════════════════════════
    //  streamFromQueue characteristics
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("streamFromQueue")
    class StreamFromQueueTest {

        @Test
        @DisplayName("trySplit returns null and characteristics include NONNULL")
        void spliteratorCharacteristics() {
            BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();
            Spliterator<String> spliterator = streamFromQueue(queue).spliterator();

            assertNull(spliterator.trySplit());
            assertTrue(spliterator.hasCharacteristics(Spliterator.ORDERED));
            assertTrue(spliterator.hasCharacteristics(Spliterator.NONNULL));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(throttleLast(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed bucket 0
            expectConsumed(consumed, 2);
            queue.put(Optional.of(new TimedEvent(0, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            awaitConsumed(consumed);
            // No results yet — bucket 0 not complete
            assertEquals(List.of(), new ArrayList<>(results));

            // Feed bucket 1 — should trigger emission of bucket 0
            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(100, "c")));
            awaitConsumed(consumed);
            assertEquals(List.of(new TimedEvent(50, "b")), new ArrayList<>(results));

            // End stream — finisher emits bucket 1
            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(bufferTime(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed bucket 0
            expectConsumed(consumed, 2);
            queue.put(Optional.of(new TimedEvent(10, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            // Feed bucket 1 — should trigger emission of bucket 0
            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(110, "c")));
            awaitConsumed(consumed);
            assertEquals(List.of(List.of(
                    new TimedEvent(10, "a"), new TimedEvent(50, "b"))), new ArrayList<>(results));

            // End stream
            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(tumblingTimeWindow(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed window [0-99]
            expectConsumed(consumed, 2);
            queue.put(Optional.of(new TimedEvent(10, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            // Feed window [100-199] — should trigger emission of window [0-99]
            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(110, "c")));
            awaitConsumed(consumed);
            assertEquals(1, results.size());
            assertEquals(0, results.get(0).startIndex());
            assertEquals(99, results.get(0).endIndex());
            assertEquals(2, results.get(0).size());

            // End stream
            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(debounce(50, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed elements within timeout
            expectConsumed(consumed, 2);
            queue.put(Optional.of(new TimedEvent(0, "a")));
            queue.put(Optional.of(new TimedEvent(30, "b")));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            // Feed element with gap > timeout — should emit "b" (pending)
            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(100, "c")));
            awaitConsumed(consumed);
            assertEquals(List.of(new TimedEvent(30, "b")), new ArrayList<>(results));

            // End stream — finisher emits "c"
            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(sessionWindow(5, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            // Feed elements within session
            expectConsumed(consumed, 2);
            queue.put(Optional.of(new TimedEvent(1, "a")));
            queue.put(Optional.of(new TimedEvent(3, "b")));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            // Feed element with gap > 5 — should close session 0
            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(20, "c")));
            awaitConsumed(consumed);
            assertEquals(1, results.size());
            assertEquals(2, results.get(0).size());

            // End stream — finisher emits session 1
            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(foldLeft(0, Integer::sum))
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 3);
            queue.put(Optional.of(1));
            queue.put(Optional.of(2));
            queue.put(Optional.of(3));
            awaitConsumed(consumed);
            // No results yet — foldLeft only emits in finisher
            assertEquals(List.of(), new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(6), new ArrayList<>(results));
        }

        @Test
        @DisplayName("reverse produces no results until stream ends")
        void reverseNoIntermediate() throws Exception {
            BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();
            List<String> results = new CopyOnWriteArrayList<>();
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(reverse())
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 2);
            queue.put(Optional.of("a"));
            queue.put(Optional.of("b"));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(List.of("b", "a"), new ArrayList<>(results));
        }

        @Test
        @DisplayName("last produces no results until stream ends")
        void lastNoIntermediate() throws Exception {
            BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();
            List<Optional<String>> results = new CopyOnWriteArrayList<>();
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(ReactiveGatherers.last())
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 2);
            queue.put(Optional.of("a"));
            queue.put(Optional.of("b"));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(Optional.of("b")), new ArrayList<>(results));
        }

        @Test
        @DisplayName("slidingTimeWindow produces no results until stream ends")
        void slidingTimeWindowNoIntermediate() throws Exception {
            BlockingQueue<Optional<TimedEvent>> queue = new LinkedBlockingQueue<>();
            List<Window<TimedEvent>> results = new CopyOnWriteArrayList<>();
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 3);
            queue.put(Optional.of(new TimedEvent(0, "a")));
            queue.put(Optional.of(new TimedEvent(50, "b")));
            queue.put(Optional.of(new TimedEvent(100, "c")));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
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
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(throttleFirst(100, TimedEvent::ts))
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(0, "a")));
            awaitConsumed(consumed);
            assertEquals(List.of(new TimedEvent(0, "a")), new ArrayList<>(results));

            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(50, "b")));
            awaitConsumed(consumed);
            assertEquals(List.of(new TimedEvent(0, "a")), new ArrayList<>(results));

            expectConsumed(consumed, 1);
            queue.put(Optional.of(new TimedEvent(100, "c")));
            awaitConsumed(consumed);
            assertEquals(List.of(new TimedEvent(0, "a"), new TimedEvent(100, "c")),
                    new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
        }

        @Test
        @DisplayName("tumblingWindow emits when window fills")
        void tumblingWindowStreaming() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<Window<Integer>> results = new CopyOnWriteArrayList<>();
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(tumblingWindow(2))
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 1);
            queue.put(Optional.of(1));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            expectConsumed(consumed, 1);
            queue.put(Optional.of(2));
            awaitConsumed(consumed);
            assertEquals(1, results.size());

            expectConsumed(consumed, 1);
            queue.put(Optional.of(3));
            awaitConsumed(consumed);
            assertEquals(1, results.size());

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(2, results.size());
        }

        @Test
        @DisplayName("grouped emits full groups and finisher emits the partial remainder")
        void groupedStreaming() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<List<Integer>> results = new CopyOnWriteArrayList<>();
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(grouped(2))
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 1);
            queue.put(Optional.of(1));
            awaitConsumed(consumed);
            assertEquals(List.of(), new ArrayList<>(results));

            expectConsumed(consumed, 1);
            queue.put(Optional.of(2));
            awaitConsumed(consumed);
            assertEquals(List.of(List.of(1, 2)), new ArrayList<>(results));

            // Odd element: remains buffered until the stream ends
            expectConsumed(consumed, 1);
            queue.put(Optional.of(3));
            awaitConsumed(consumed);
            assertEquals(List.of(List.of(1, 2)), new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(List.of(1, 2), List.of(3)), new ArrayList<>(results));
        }

        @Test
        @DisplayName("scanLeft emits each step incrementally")
        void scanLeftStreaming() throws Exception {
            BlockingQueue<Optional<Integer>> queue = new LinkedBlockingQueue<>();
            List<Integer> results = new CopyOnWriteArrayList<>();
            AtomicReference<CountDownLatch> consumed = new AtomicReference<>(new CountDownLatch(0));
            CountDownLatch done = new CountDownLatch(1);

            Thread.startVirtualThread(() -> {
                streamFromQueue(queue, consumed)
                    .gather(scanLeft(0, Integer::sum))
                    .forEach(results::add);
                done.countDown();
            });

            expectConsumed(consumed, 1);
            queue.put(Optional.of(1));
            awaitConsumed(consumed);
            assertEquals(List.of(0, 1), new ArrayList<>(results));

            expectConsumed(consumed, 1);
            queue.put(Optional.of(2));
            awaitConsumed(consumed);
            assertEquals(List.of(0, 1, 3), new ArrayList<>(results));

            queue.put(Optional.empty());
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(0, 1, 3), new ArrayList<>(results));
        }
    }
}
