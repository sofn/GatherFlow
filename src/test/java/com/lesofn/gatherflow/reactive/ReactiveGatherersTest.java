package com.lesofn.gatherflow.reactive;

import com.lesofn.gatherflow.reactive.ReactiveGatherers.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Stream;

import static com.lesofn.gatherflow.reactive.ReactiveGatherers.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReactiveGatherers")
class ReactiveGatherersTest {

    record TimedEvent(long ts, String value) {}

    // ═══════════════════════════════════════════════
    //  Debounce
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("debounce")
    class DebounceTest {

        @Test
        @DisplayName("emit last element per debounce period")
        void debounceBasic() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(50, "b"),
                    new TimedEvent(80, "c"), new TimedEvent(200, "d"),
                    new TimedEvent(230, "e"), new TimedEvent(300, "f")
            ).gather(debounce(100, TimedEvent::ts))
                    .toList();
            // Gap 80→200 = 120 > 100, so emit c; gap 230→300 = 70 < 100, no emit until finish
            assertEquals(List.of(new TimedEvent(80, "c"), new TimedEvent(300, "f")), result);
        }

        @Test
        @DisplayName("all within timeout emits only last")
        void allWithinTimeout() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(10, "b"), new TimedEvent(20, "c")
            ).gather(debounce(100, TimedEvent::ts))
                    .toList();
            assertEquals(List.of(new TimedEvent(20, "c")), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<TimedEvent> result = Stream.<TimedEvent>empty()
                    .gather(debounce(100, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("single element")
        void singleElement() {
            List<TimedEvent> result = Stream.of(new TimedEvent(0, "a"))
                    .gather(debounce(100, TimedEvent::ts))
                    .toList();
            assertEquals(List.of(new TimedEvent(0, "a")), result);
        }

        @Test
        @DisplayName("each element isolated by gap")
        void eachIsolated() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(200, "b"), new TimedEvent(400, "c")
            ).gather(debounce(100, TimedEvent::ts))
                    .toList();
            assertEquals(3, result.size());
        }

        @Test
        @DisplayName("negative timeout throws")
        void negativeTimeout() {
            assertThrows(IllegalArgumentException.class, () -> debounce(-1, TimedEvent::ts));
        }

        @Test
        @DisplayName("null extractor throws")
        void nullExtractor() {
            assertThrows(NullPointerException.class, () -> debounce(100, null));
        }
    }

    // ═══════════════════════════════════════════════
    //  ThrottleFirst
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("throttleFirst")
    class ThrottleFirstTest {

        @Test
        @DisplayName("emit first per time window")
        void basic() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(10, "b"),
                    new TimedEvent(50, "c"), new TimedEvent(100, "d"),
                    new TimedEvent(110, "e"), new TimedEvent(150, "f")
            ).gather(throttleFirst(100, TimedEvent::ts))
                    .toList();
            assertEquals(List.of(new TimedEvent(0, "a"), new TimedEvent(100, "d")), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<TimedEvent> result = Stream.<TimedEvent>empty()
                    .gather(throttleFirst(100, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid windowSize throws")
        void invalidSize() {
            assertThrows(IllegalArgumentException.class, () -> throttleFirst(0, TimedEvent::ts));
        }
    }

    // ═══════════════════════════════════════════════
    //  ThrottleLast / Sample
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("throttleLast")
    class ThrottleLastTest {

        @Test
        @DisplayName("emit last per time window")
        void basic() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(10, "b"),
                    new TimedEvent(50, "c"), new TimedEvent(100, "d"),
                    new TimedEvent(110, "e"), new TimedEvent(150, "f")
            ).gather(throttleLast(100, TimedEvent::ts))
                    .toList();
            assertEquals(List.of(new TimedEvent(50, "c"), new TimedEvent(150, "f")), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<TimedEvent> result = Stream.<TimedEvent>empty()
                    .gather(throttleLast(100, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid windowSize throws")
        void invalidSize() {
            assertThrows(IllegalArgumentException.class, () -> throttleLast(0, TimedEvent::ts));
        }
    }

    // ═══════════════════════════════════════════════
    //  BufferTime
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("bufferTime")
    class BufferTimeTest {

        @Test
        @DisplayName("buffer elements by time bucket")
        void basic() {
            List<List<TimedEvent>> result = Stream.of(
                    new TimedEvent(10, "a"), new TimedEvent(50, "b"),
                    new TimedEvent(110, "c"), new TimedEvent(150, "d")
            ).gather(bufferTime(100, TimedEvent::ts))
                    .toList();
            assertEquals(2, result.size());
            assertEquals(2, result.get(0).size()); // a, b (bucket 0)
            assertEquals(2, result.get(1).size()); // c, d (bucket 100)
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<List<TimedEvent>> result = Stream.<TimedEvent>empty()
                    .gather(bufferTime(100, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid timespan throws")
        void invalidTimespan() {
            assertThrows(IllegalArgumentException.class, () -> bufferTime(0, TimedEvent::ts));
        }
    }

    // ═══════════════════════════════════════════════
    //  Timestamp
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("timestamp")
    class TimestampTest {

        @Test
        @DisplayName("attach timestamp to each element")
        void basic() {
            List<Timestamped<TimedEvent>> result = Stream.of(
                    new TimedEvent(10, "a"), new TimedEvent(50, "b")
            ).gather(timestamp(TimedEvent::ts))
                    .toList();
            assertEquals(2, result.size());
            assertEquals(10, result.get(0).timestamp());
            assertEquals(new TimedEvent(10, "a"), result.get(0).value());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Timestamped<TimedEvent>> result = Stream.<TimedEvent>empty()
                    .gather(timestamp(TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  TimeInterval
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("timeInterval")
    class TimeIntervalTest {

        @Test
        @DisplayName("measure elapsed time between elements")
        void basic() {
            List<Timed<TimedEvent>> result = Stream.of(
                    new TimedEvent(10, "a"), new TimedEvent(50, "b"), new TimedEvent(80, "c")
            ).gather(timeInterval(TimedEvent::ts))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(0, result.get(0).elapsedMillis()); // first = 0
            assertEquals(40, result.get(1).elapsedMillis()); // 50-10
            assertEquals(30, result.get(2).elapsedMillis()); // 80-50
        }

        @Test
        @DisplayName("single element has elapsed 0")
        void singleElement() {
            List<Timed<TimedEvent>> result = Stream.of(new TimedEvent(10, "a"))
                    .gather(timeInterval(TimedEvent::ts))
                    .toList();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).elapsedMillis());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Timed<TimedEvent>> result = Stream.<TimedEvent>empty()
                    .gather(timeInterval(TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  DoOnNext
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("doOnNext")
    class DoOnNextTest {

        @Test
        @DisplayName("side-effect on each element")
        void basic() {
            List<Integer> sideEffects = new ArrayList<>();
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(doOnNext(sideEffects::add))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
            assertEquals(List.of(1, 2, 3), sideEffects);
        }

        @Test
        @DisplayName("empty stream no side-effect")
        void emptyStream() {
            List<Integer> sideEffects = new ArrayList<>();
            List<Integer> result = Stream.<Integer>empty()
                    .gather(doOnNext(sideEffects::add))
                    .toList();
            assertTrue(result.isEmpty());
            assertTrue(sideEffects.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  DoOnComplete
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("doOnComplete")
    class DoOnCompleteTest {

        @Test
        @DisplayName("side-effect on stream completion")
        void basic() {
            AtomicBoolean completed = new AtomicBoolean(false);
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(doOnComplete(() -> completed.set(true)))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
            assertTrue(completed.get());
        }

        @Test
        @DisplayName("fires even on empty stream")
        void emptyStream() {
            AtomicBoolean completed = new AtomicBoolean(false);
            List<Integer> result = Stream.<Integer>empty()
                    .gather(doOnComplete(() -> completed.set(true)))
                    .toList();
            assertTrue(result.isEmpty());
            assertTrue(completed.get());
        }
    }

    // ═══════════════════════════════════════════════
    //  DoOnError
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("doOnError")
    class DoOnErrorTest {

        @Test
        @DisplayName("observes error and re-throws")
        void observesError() {
            List<Throwable> errors = new ArrayList<>();
            assertThrows(Exception.class, () -> {
                Stream.of(1, 2, 3)
                        .gather(doOnError(errors::add))
                        .map(x -> { if (x == 2) throw new RuntimeException("boom"); return x; })
                        .toList();
            });
        }

        @Test
        @DisplayName("no error no side-effect")
        void noError() {
            List<Throwable> errors = new ArrayList<>();
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(doOnError(errors::add))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
            assertTrue(errors.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  DoFinally
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("doFinally")
    class DoFinallyTest {

        @Test
        @DisplayName("fires on normal completion")
        void normalCompletion() {
            AtomicReference<String> signal = new AtomicReference<>();
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(doFinally(signal::set))
                    .toList();
            assertEquals("complete", signal.get());
        }

        @Test
        @DisplayName("fires on empty stream")
        void emptyStream() {
            AtomicReference<String> signal = new AtomicReference<>();
            Stream.<Integer>empty().gather(doFinally(signal::set)).toList();
            assertEquals("complete", signal.get());
        }
    }

    // ═══════════════════════════════════════════════
    //  Materialize / Dematerialize
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("materialize and dematerialize")
    class MaterializeTest {

        @Test
        @DisplayName("materialize produces OnNext + OnComplete")
        void materialize() {
            List<Notification<String>> result = Stream.of("a", "b")
                    .gather(ReactiveGatherers.<String>materialize())
                    .toList();
            assertEquals(3, result.size());
            assertInstanceOf(Notification.OnNext.class, result.get(0));
            assertEquals("a", ((Notification.OnNext<String>) result.get(0)).value());
            assertInstanceOf(Notification.OnComplete.class, result.get(2));
        }

        @Test
        @DisplayName("materialize empty stream produces only OnComplete")
        void materializeEmpty() {
            List<Notification<String>> result = Stream.<String>empty()
                    .gather(ReactiveGatherers.<String>materialize())
                    .toList();
            assertEquals(1, result.size());
            assertInstanceOf(Notification.OnComplete.class, result.getFirst());
        }

        @Test
        @DisplayName("dematerialize reverses materialize")
        void roundTrip() {
            List<String> result = Stream.of("a", "b", "c")
                    .gather(ReactiveGatherers.<String>materialize())
                    .gather(dematerialize())
                    .toList();
            assertEquals(List.of("a", "b", "c"), result);
        }

        @Test
        @DisplayName("dematerialize OnError throws")
        void dematerializeOnError() {
            assertThrows(RuntimeException.class, () -> {
                Stream.of(new Notification.OnError<String>(new RuntimeException("boom")))
                        .gather(dematerialize())
                        .toList();
            });
        }

        @Test
        @DisplayName("dematerialize OnComplete is consumed silently")
        void dematerializeOnComplete() {
            List<String> result = Stream.of(
                    new Notification.OnNext<>("a"),
                    new Notification.OnComplete<String>()
            ).gather(dematerialize())
                    .toList();
            assertEquals(List.of("a"), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  OnErrorReturn
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("onErrorReturn")
    class OnErrorReturnTest {

        @Test
        @DisplayName("fallback value on mapping error")
        void fallback() {
            List<String> result = Stream.of(1, 0, 2)
                    .gather(onErrorReturn(
                            i -> 10 / i == 5 ? "five" : String.valueOf(10 / i),
                            "ERROR"))
                    .toList();
            assertEquals("ERROR", result.get(1)); // 10/0 throws
        }

        @Test
        @DisplayName("no error passes through")
        void noError() {
            List<Integer> result = Stream.of(2, 4, 6)
                    .gather(onErrorReturn(i -> i / 2, -1))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  OnErrorResume
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("onErrorResume")
    class OnErrorResumeTest {

        @Test
        @DisplayName("fallback iterable on mapping error")
        void fallback() {
            List<Integer> result = Stream.of(2, 0, 4)
                    .gather(onErrorResume(
                            i -> 100 / i,
                            e -> List.of(-1)))
                    .toList();
            assertEquals(List.of(50, -1, 25), result);
        }

        @Test
        @DisplayName("no error passes through")
        void noError() {
            List<Integer> result = Stream.of(2, 4)
                    .gather(onErrorResume(
                            i -> 100 / i,
                            e -> List.of(-1)))
                    .toList();
            assertEquals(List.of(50, 25), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Retry
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("retry")
    class RetryTest {

        @Test
        @DisplayName("succeeds after retry")
        void succeedsAfterRetry() {
            AtomicInteger attempt = new AtomicInteger(0);
            List<String> result = Stream.of(1, 2, 3)
                    .gather(retry(i -> {
                        attempt.incrementAndGet();
                        if (i == 2 && attempt.get() <= 2) throw new RuntimeException("transient");
                        return "v" + i;
                    }, 3))
                    .toList();
            // Element 1: attempt=1, success; Element 2: attempt=2 fail, attempt=3 success
            assertEquals(List.of("v1", "v2", "v3"), result);
        }

        @Test
        @DisplayName("exhausted retries throws")
        void exhaustedRetries() {
            assertThrows(RuntimeException.class, () -> {
                Stream.of(1).gather(retry(i -> {
                    throw new RuntimeException("always fails");
                }, 2)).toList();
            });
        }

        @Test
        @DisplayName("zero retries means no retry")
        void zeroRetries() {
            assertThrows(RuntimeException.class, () -> {
                Stream.of(1).gather(retry(i -> {
                    throw new RuntimeException("fail");
                }, 0)).toList();
            });
        }

        @Test
        @DisplayName("negative maxRetries throws")
        void negativeRetries() {
            assertThrows(IllegalArgumentException.class, () -> retry(i -> i, -1));
        }

        @Test
        @DisplayName("no error no retry needed")
        void noError() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(retry(i -> i * 10, 3))
                    .toList();
            assertEquals(List.of(10, 20, 30), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Repeat
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("repeat")
    class RepeatTest {

        @Test
        @DisplayName("repeat 2 times")
        void repeat2() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(repeat(2))
                    .toList();
            assertEquals(List.of(1, 2, 3, 1, 2, 3), result);
        }

        @Test
        @DisplayName("repeat 1 is identity")
        void repeat1() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(repeat(1))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("empty stream repeat is empty")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(repeat(3))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid times throws")
        void invalidTimes() {
            assertThrows(IllegalArgumentException.class, () -> repeat(0));
        }
    }

    // ═══════════════════════════════════════════════
    //  DefaultIfEmpty
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("defaultIfEmpty")
    class DefaultIfEmptyTest {

        @Test
        @DisplayName("non-empty stream passes through")
        void nonEmpty() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(defaultIfEmpty(42))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("empty stream emits default")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(defaultIfEmpty(42))
                    .toList();
            assertEquals(List.of(42), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  SwitchIfEmpty
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("switchIfEmpty")
    class SwitchIfEmptyTest {

        @Test
        @DisplayName("non-empty stream passes through")
        void nonEmpty() {
            List<Integer> result = Stream.of(1, 2)
                    .gather(switchIfEmpty(List.of(99)))
                    .toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        @DisplayName("empty stream switches to fallback")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(switchIfEmpty(List.of(10, 20)))
                    .toList();
            assertEquals(List.of(10, 20), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  StartWith
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("startWith")
    class StartWithTest {

        @Test
        @DisplayName("prepend elements")
        void prepend() {
            List<Integer> result = Stream.of(3, 4)
                    .gather(startWith(List.of(1, 2)))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4), result);
        }

        @Test
        @DisplayName("empty stream emits only prefix")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(startWith(List.of(1, 2)))
                    .toList();
            assertEquals(List.of(1, 2), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  ConcatWith
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("concatWith")
    class ConcatWithTest {

        @Test
        @DisplayName("append elements")
        void append() {
            List<Integer> result = Stream.of(1, 2)
                    .gather(concatWith(List.of(3, 4)))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4), result);
        }

        @Test
        @DisplayName("empty stream emits only suffix")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(concatWith(List.of(3, 4)))
                    .toList();
            assertEquals(List.of(3, 4), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Delay
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("delay")
    class DelayTest {

        @Test
        @DisplayName("reorders by timestamp")
        void reorder() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(200, "c"), new TimedEvent(0, "a"), new TimedEvent(100, "b")
            ).gather(delay(TimedEvent::ts))
                    .toList();
            assertEquals(List.of(new TimedEvent(0, "a"), new TimedEvent(100, "b"), new TimedEvent(200, "c")), result);
        }

        @Test
        @DisplayName("already ordered stays same")
        void alreadyOrdered() {
            List<TimedEvent> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(100, "b")
            ).gather(delay(TimedEvent::ts))
                    .toList();
            assertEquals(List.of(new TimedEvent(0, "a"), new TimedEvent(100, "b")), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<TimedEvent> result = Stream.<TimedEvent>empty()
                    .gather(delay(TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  ElementAt
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("elementAt")
    class ElementAtTest {

        @Test
        @DisplayName("get element at index 2")
        void atIndex2() {
            Optional<String> result = Stream.of("a", "b", "c", "d")
                    .gather(elementAt(2))
                    .findFirst()
                    .orElseThrow();
            assertEquals(Optional.of("c"), result);
        }

        @Test
        @DisplayName("out of bounds returns empty")
        void outOfBounds() {
            Optional<String> result = Stream.of("a", "b")
                    .gather(elementAt(5))
                    .findFirst()
                    .orElseThrow();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("empty stream returns empty")
        void emptyStream() {
            Optional<String> result = Stream.<String>empty()
                    .gather(elementAt(0))
                    .findFirst()
                    .orElseThrow();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("negative index throws")
        void negativeIndex() {
            assertThrows(IllegalArgumentException.class, () -> elementAt(-1));
        }
    }

    // ═══════════════════════════════════════════════
    //  First
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("first")
    class FirstTest {

        @Test
        @DisplayName("get first element")
        void basic() {
            Optional<Integer> result = Stream.of(10, 20, 30)
                    .gather(first())
                    .findFirst()
                    .orElseThrow();
            assertEquals(Optional.of(10), result);
        }

        @Test
        @DisplayName("empty stream returns empty")
        void emptyStream() {
            Optional<Integer> result = Stream.<Integer>empty()
                    .gather(first())
                    .findFirst()
                    .orElseThrow();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Last
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("last")
    class LastTest {

        @Test
        @DisplayName("get last element")
        void basic() {
            Optional<Integer> result = Stream.of(10, 20, 30)
                    .gather(last())
                    .findFirst()
                    .orElseThrow();
            assertEquals(Optional.of(30), result);
        }

        @Test
        @DisplayName("empty stream returns empty")
        void emptyStream() {
            Optional<Integer> result = Stream.<Integer>empty()
                    .gather(last())
                    .findFirst()
                    .orElseThrow();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  SkipLast
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("skipLast")
    class SkipLastTest {

        @Test
        @DisplayName("skip last 2")
        void skipLast2() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(skipLast(2))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("skip last 0 is identity")
        void skipLast0() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(skipLast(0))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("skip more than size produces empty")
        void skipMoreThanSize() {
            List<Integer> result = Stream.of(1, 2)
                    .gather(skipLast(5))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("negative throws")
        void negativeThrows() {
            assertThrows(IllegalArgumentException.class, () -> skipLast(-1));
        }
    }

    // ═══════════════════════════════════════════════
    //  TakeLast
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("takeLast")
    class TakeLastTest {

        @Test
        @DisplayName("take last 2")
        void takeLast2() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(takeLast(2))
                    .toList();
            assertEquals(List.of(4, 5), result);
        }

        @Test
        @DisplayName("take last 0 produces empty")
        void takeLast0() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(takeLast(0))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("take last more than size takes all")
        void takeLastMoreThanSize() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(takeLast(10))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("negative throws")
        void negativeThrows() {
            assertThrows(IllegalArgumentException.class, () -> takeLast(-1));
        }
    }

    // ═══════════════════════════════════════════════
    //  DistinctUntilChanged
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("distinctUntilChanged")
    class DistinctUntilChangedTest {

        @Test
        @DisplayName("suppress consecutive duplicates")
        void basic() {
            List<Integer> result = Stream.of(1, 1, 2, 2, 3, 1, 1)
                    .gather(distinctUntilChanged())
                    .toList();
            assertEquals(List.of(1, 2, 3, 1), result);
        }

        @Test
        @DisplayName("no consecutive duplicates keeps all")
        void noDuplicates() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(distinctUntilChanged())
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(distinctUntilChanged())
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("by key selector")
        void byKey() {
            // "aa"(2), "ab"(2=dup), "bb"(2=dup), "bc"(2=dup), "cc"(2=dup)
            // All have length 2, so only first is emitted
            List<String> result = Stream.of("aa", "ab", "bb", "bc", "cc")
                    .gather(distinctUntilChanged(String::length))
                    .toList();
            assertEquals(List.of("aa"), result);
        }

        @Test
        @DisplayName("by key selector with key changes")
        void byKeyWithChanges() {
            List<String> result = Stream.of("a", "bb", "ccc", "dd", "e")
                    .gather(distinctUntilChanged(String::length))
                    .toList();
            // a(1), bb(2), ccc(3), dd(2), e(1) — all different consecutive keys
            assertEquals(List.of("a", "bb", "ccc", "dd", "e"), result);
        }

        @Test
        @DisplayName("null values handled correctly")
        void nullValues() {
            // distinctUntilChanged uses Objects.equals which handles null
            List<String> result = Stream.of("a", "a", null, null, "b")
                    .gather(distinctUntilChanged())
                    .toList();
            // "a"→emit, "a"→dup, null→emit(diff from "a"), null→dup, "b"→emit
            assertEquals(3, result.size());
            assertEquals("a", result.get(0));
            assertNull(result.get(1));
            assertEquals("b", result.get(2));
        }
    }

    // ═══════════════════════════════════════════════
    //  WithLatestFrom
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("withLatestFrom")
    class WithLatestFromTest {

        @Test
        @DisplayName("combine with latest from other")
        void basic() {
            List<String> result = Stream.of(1, 2, 3)
                    .gather(withLatestFrom(List.of("a", "b", "c"), (i, s) -> i + ":" + s))
                    .toList();
            assertEquals(List.of("1:a", "2:b", "3:c"), result);
        }

        @Test
        @DisplayName("other shorter than main uses last value")
        void otherShorter() {
            List<String> result = Stream.of(1, 2, 3)
                    .gather(withLatestFrom(List.of("a"), (i, s) -> i + ":" + s))
                    .toList();
            // 1→"a", 2→"a"(no more from other, uses latest), 3→"a"
            assertEquals(List.of("1:a", "2:a", "3:a"), result);
        }

        @Test
        @DisplayName("empty other skips all")
        void emptyOther() {
            List<String> result = Stream.of(1, 2, 3)
                    .gather(withLatestFrom(List.of(), (i, s) -> i + ":" + s))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Scan
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("scan")
    class ScanTest {

        @Test
        @DisplayName("cumulative sum with seed")
        void cumulativeSum() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(scan(0, Integer::sum))
                    .toList();
            assertEquals(List.of(0, 1, 3, 6), result);
        }

        @Test
        @DisplayName("empty stream emits seed")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(scan(42, Integer::sum))
                    .toList();
            assertEquals(List.of(42), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  ReduceWith
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("reduceWith")
    class ReduceWithTest {

        @Test
        @DisplayName("sum with seed")
        void sum() {
            Integer result = Stream.of(1, 2, 3)
                    .gather(reduceWith(0, Integer::sum))
                    .findFirst().orElseThrow();
            assertEquals(6, result);
        }

        @Test
        @DisplayName("empty stream returns seed")
        void emptyStream() {
            Integer result = Stream.<Integer>empty()
                    .gather(reduceWith(42, Integer::sum))
                    .findFirst().orElseThrow();
            assertEquals(42, result);
        }
    }

    // ═══════════════════════════════════════════════
    //  CollectList
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("collectList")
    class CollectListTest {

        @Test
        @DisplayName("collect all elements")
        void basic() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(collectList())
                    .findFirst().orElseThrow();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("empty stream produces empty list")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(collectList())
                    .findFirst().orElseThrow();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  MapWithIndex
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("mapWithIndex")
    class MapWithIndexTest {

        @Test
        @DisplayName("pair with index using combiner")
        void basic() {
            List<String> result = Stream.of("a", "b", "c")
                    .gather(mapWithIndex((idx, val) -> idx + ":" + val))
                    .toList();
            assertEquals(List.of("0:a", "1:b", "2:c"), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<String> result = Stream.<String>empty()
                    .gather(mapWithIndex((idx, val) -> idx + ":" + val))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Notification types
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("Notification types")
    class NotificationTypeTest {

        @Test
        @DisplayName("OnNext null value throws")
        void onNextNull() {
            assertThrows(NullPointerException.class, () -> new Notification.OnNext<>(null));
        }

        @Test
        @DisplayName("OnError null error throws")
        void onErrorNull() {
            assertThrows(NullPointerException.class, () -> new Notification.OnError<>(null));
        }

        @Test
        @DisplayName("OnNext value accessor")
        void onNextValue() {
            var n = new Notification.OnNext<>("hello");
            assertEquals("hello", n.value());
        }

        @Test
        @DisplayName("OnError error accessor")
        void onErrorAccessor() {
            var ex = new RuntimeException("test");
            var n = new Notification.OnError<>(ex);
            assertEquals(ex, n.error());
        }

        @Test
        @DisplayName("OnComplete is a singleton-like record")
        void onComplete() {
            var n = new Notification.OnComplete<String>();
            assertNotNull(n);
        }
    }

    // ═══════════════════════════════════════════════
    //  Timestamped / Timed records
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("Timestamped and Timed records")
    class RecordTest {

        @Test
        @DisplayName("Timestamped accessors")
        void timestamped() {
            var t = new Timestamped<>(100L, "val");
            assertEquals(100L, t.timestamp());
            assertEquals("val", t.value());
        }

        @Test
        @DisplayName("Timed accessors")
        void timed() {
            var t = new Timed<>(50L, "val");
            assertEquals(50L, t.elapsedMillis());
            assertEquals("val", t.value());
        }
    }

    // ═══════════════════════════════════════════════
    //  Missing branch coverage tests
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("Missing branch coverage")
    class MissingBranchCoverageTest {

        @Test
        @DisplayName("onErrorResume catch branch — mapper throws, fallback is used")
        void onErrorResumeCatchBranch() {
            // The catch branch in onErrorResume: mapper throws, fallbackFactory is called
            List<String> result = Stream.of("ok", "boom", "fine")
                    .gather(onErrorResume(
                            s -> {
                                if (s.equals("boom")) throw new RuntimeException("explosion");
                                return s.toUpperCase();
                            },
                            e -> List.of("FALLBACK:" + e.getMessage())
                    ))
                    .toList();
            assertEquals(List.of("OK", "FALLBACK:explosion", "FINE"), result);
        }

        @Test
        @DisplayName("onErrorResume fallback with short-circuit (downstream.push returns false)")
        void onErrorResumeShortCircuit() {
            List<String> result = Stream.of("boom")
                    .gather(onErrorResume(
                            s -> { throw new RuntimeException("fail"); },
                            e -> List.of("a", "b", "c")
                    ))
                    .limit(1)
                    .toList();
            assertEquals(List.of("a"), result);
        }
    }
}
