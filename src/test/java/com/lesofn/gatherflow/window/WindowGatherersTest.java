package com.lesofn.gatherflow.window;

import com.lesofn.gatherflow.window.WindowGatherers.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.lesofn.gatherflow.window.WindowGatherers.*;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("WindowGatherers")
class WindowGatherersTest {

    // Helper: timed event for time-based windows
    record TimedEvent(long ts, String value) {}
    record Event(String key, int value) {}

    // ═══════════════════════════════════════════════
    //  Tumbling Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("tumblingWindow")
    class TumblingWindowTest {

        @Test
        @DisplayName("exact fit produces full windows")
        void exactFit() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5, 6)
                    .gather(tumblingWindow(2))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(List.of(1, 2), result.get(0).elements());
            assertEquals(List.of(3, 4), result.get(1).elements());
            assertEquals(List.of(5, 6), result.get(2).elements());
        }

        @Test
        @DisplayName("remainder produces partial window")
        void withRemainder() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(tumblingWindow(2))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(List.of(5), result.get(2).elements());
        }

        @Test
        @DisplayName("window metadata is correct")
        void metadata() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(tumblingWindow(2))
                    .toList();
            assertEquals(0, result.get(0).windowId());
            assertEquals(0, result.get(0).startIndex());
            assertEquals(1, result.get(0).endIndex());
            assertEquals(1, result.get(1).windowId());
            assertEquals(2, result.get(1).startIndex());
            assertEquals(3, result.get(1).endIndex());
            assertEquals(2, result.get(2).windowId());
            assertEquals(4, result.get(2).startIndex());
            assertEquals(4, result.get(2).endIndex());
        }

        @Test
        @DisplayName("size 1 wraps each element")
        void sizeOne() {
            List<Window<Integer>> result = Stream.of(1, 2, 3)
                    .gather(tumblingWindow(1))
                    .toList();
            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(w -> w.size() == 1));
        }

        @Test
        @DisplayName("empty stream produces no windows")
        void emptyStream() {
            List<Window<Integer>> result = Stream.<Integer>empty()
                    .gather(tumblingWindow(2))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid size throws")
        void invalidSize() {
            assertThrows(IllegalArgumentException.class, () -> tumblingWindow(0));
        }
    }

    // ═══════════════════════════════════════════════
    //  Sliding Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("slidingWindow")
    class SlidingWindowTest {

        @Test
        @DisplayName("sliding window step 1")
        void stepOne() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(slidingWindow(3))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(List.of(1, 2, 3), result.get(0).elements());
            assertEquals(List.of(2, 3, 4), result.get(1).elements());
            assertEquals(List.of(3, 4, 5), result.get(2).elements());
        }

        @Test
        @DisplayName("sliding window step 2")
        void stepTwo() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(slidingWindow(3, 2))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(List.of(1, 2, 3), result.get(0).elements());
            assertEquals(List.of(3, 4, 5), result.get(1).elements());
            assertEquals(List.of(5), result.get(2).elements());
            assertEquals(2, result.get(2).windowId());
            assertEquals(4, result.get(2).startIndex());
            assertEquals(4, result.get(2).endIndex());
        }

        @Test
        @DisplayName("window metadata for sliding")
        void metadata() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(slidingWindow(3))
                    .toList();
            assertEquals(0, result.get(0).windowId());
            assertEquals(0, result.get(0).startIndex());
            assertEquals(2, result.get(0).endIndex());
            assertEquals(1, result.get(1).windowId());
            assertEquals(1, result.get(1).startIndex());
            assertEquals(3, result.get(1).endIndex());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Window<Integer>> result = Stream.<Integer>empty()
                    .gather(slidingWindow(3))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid params throw")
        void invalidParams() {
            assertThrows(IllegalArgumentException.class, () -> slidingWindow(0));
            assertThrows(IllegalArgumentException.class, () -> slidingWindow(2, 0));
        }

        @Test
        @DisplayName("large input sliding performance")
        void largeInputPerformance() {
            assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                    IntStream.range(0, 100_000).boxed()
                            .gather(slidingWindow(1000))
                            .skip(99_000)
                            .limit(1)
                            .toList()
            );
        }
    }

    // ═══════════════════════════════════════════════
    //  Session Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("sessionWindow")
    class SessionWindowTest {

        @Test
        @DisplayName("two sessions separated by gap")
        void twoSessions() {
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(1, "a"), new TimedEvent(2, "b"),
                    new TimedEvent(10, "c"), new TimedEvent(11, "d")
            ).gather(sessionWindow(5, TimedEvent::ts))
                    .toList();
            assertEquals(2, result.size());
            assertEquals(List.of(new TimedEvent(1, "a"), new TimedEvent(2, "b")),
                    result.get(0).elements());
            assertEquals(List.of(new TimedEvent(10, "c"), new TimedEvent(11, "d")),
                    result.get(1).elements());
        }

        @Test
        @DisplayName("all within gap produces single session")
        void singleSession() {
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(1, "a"), new TimedEvent(3, "b"), new TimedEvent(5, "c")
            ).gather(sessionWindow(5, TimedEvent::ts))
                    .toList();
            assertEquals(1, result.size());
            assertEquals(3, result.get(0).size());
        }

        @Test
        @DisplayName("each element isolated by gap")
        void isolatedElements() {
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(1, "a"), new TimedEvent(100, "b"), new TimedEvent(200, "c")
            ).gather(sessionWindow(5, TimedEvent::ts))
                    .toList();
            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(w -> w.size() == 1));
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Window<TimedEvent>> result = Stream.<TimedEvent>empty()
                    .gather(sessionWindow(5, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("single element")
        void singleElement() {
            List<Window<TimedEvent>> result = Stream.of(new TimedEvent(1, "a"))
                    .gather(sessionWindow(5, TimedEvent::ts))
                    .toList();
            assertEquals(1, result.size());
            assertEquals(1, result.get(0).size());
        }

        @Test
        @DisplayName("negative gap throws")
        void negativeGap() {
            assertThrows(IllegalArgumentException.class,
                    () -> sessionWindow(-1, TimedEvent::ts));
        }
    }

    // ═══════════════════════════════════════════════
    //  Global Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("globalWindow")
    class GlobalWindowTest {

        @Test
        @DisplayName("all elements in one window")
        void allInOne() {
            List<Window<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(globalWindow())
                    .toList();
            assertEquals(1, result.size());
            assertEquals(List.of(1, 2, 3, 4, 5), result.get(0).elements());
            assertEquals(0, result.get(0).startIndex());
            assertEquals(4, result.get(0).endIndex());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Window<Integer>> result = Stream.<Integer>empty()
                    .gather(globalWindow())
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Reduce
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowReduce")
    class WindowReduceTest {

        @Test
        @DisplayName("sum within each window")
        void sumPerWindow() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6)
                    .gather(tumblingWindow(2))
                    .gather(windowReduce(Integer::sum))
                    .toList();
            assertEquals(List.of(3, 7, 11), result);
        }

        @Test
        @DisplayName("product within each window")
        void productPerWindow() {
            List<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(tumblingWindow(2))
                    .gather(windowReduce((a, b) -> a * b))
                    .toList();
            assertEquals(List.of(2, 12), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Aggregate
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowAggregate")
    class WindowAggregateTest {

        @Test
        @DisplayName("average within each window")
        void averagePerWindow() {
            List<Double> result = Stream.of(10, 20, 30, 40, 50)
                    .gather(tumblingWindow(2))
                    .gather(windowAggregate(
                            () -> new double[]{0, 0},
                            (acc, x) -> new double[]{acc[0] + x, acc[1] + 1},
                            acc -> acc[0] / acc[1]
                    ))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(15.0, result.get(0), 0.001);
            assertEquals(35.0, result.get(1), 0.001);
            assertEquals(50.0, result.get(2), 0.001);
        }

        @Test
        @DisplayName("string concatenation per window")
        void stringConcat() {
            List<String> result = Stream.of("a", "b", "c", "d")
                    .gather(tumblingWindow(2))
                    .gather(windowAggregate(
                            () -> new StringBuilder(),
                            StringBuilder::append,
                            StringBuilder::toString
                    ))
                    .toList();
            assertEquals(List.of("ab", "cd"), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Process
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowProcess")
    class WindowProcessTest {

        @Test
        @DisplayName("process with full window context")
        void processWithContext() {
            List<String> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(tumblingWindow(2))
                    .gather(windowProcess(window ->
                            List.of("Window#" + window.windowId()
                                    + " size=" + window.size()
                                    + " sum=" + window.elements().stream().mapToInt(i -> i).sum())))
                    .toList();
            assertEquals(List.of("Window#0 size=2 sum=3", "Window#1 size=2 sum=7", "Window#2 size=1 sum=5"), result);
        }

        @Test
        @DisplayName("process can emit multiple results per window")
        void multiEmit() {
            List<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(tumblingWindow(2))
                    .gather(windowProcess(window -> window.elements()))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  KeyBy
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("keyBy")
    class KeyByTest {

        @Test
        @DisplayName("tag elements with key")
        void tagWithKey() {
            List<KeyedResult<Integer, String>> result = Stream.of("aa", "bb", "c")
                    .gather(keyBy(String::length))
                    .toList();
            assertEquals(3, result.size());
            assertEquals(new KeyedResult<>(2, "aa"), result.get(0));
            assertEquals(new KeyedResult<>(2, "bb"), result.get(1));
            assertEquals(new KeyedResult<>(1, "c"), result.get(2));
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<KeyedResult<Integer, String>> result = Stream.<String>empty()
                    .gather(keyBy(String::length))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Keyed Tumbling Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("keyedTumblingWindow")
    class KeyedTumblingWindowTest {

        @Test
        @DisplayName("window per key independently")
        void perKey() {
            List<KeyedResult<String, Window<Event>>> result = Stream.of(
                    new Event("A", 1), new Event("B", 10),
                    new Event("A", 2), new Event("B", 20),
                    new Event("A", 3), new Event("B", 30)
            ).gather(keyedTumblingWindow(Event::key, 2))
                    .toList();

            // Key A: window [1,2], then partial [3]
            // Key B: window [10,20], then partial [30]
            assertEquals(4, result.size());

            // First A window
            KeyedResult<String, Window<Event>> firstA = result.stream()
                    .filter(r -> r.key().equals("A")).findFirst().orElseThrow();
            assertEquals(List.of(new Event("A", 1), new Event("A", 2)), firstA.result().elements());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<KeyedResult<String, Window<Event>>> result = Stream.<Event>empty()
                    .gather(keyedTumblingWindow(Event::key, 2))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Keyed Window Reduce
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("keyedWindowReduce")
    class KeyedWindowReduceTest {

        @Test
        @DisplayName("reduce per keyed window")
        void reducePerKey() {
            List<KeyedResult<String, Event>> result = Stream.of(
                    new Event("A", 1), new Event("B", 10),
                    new Event("A", 2), new Event("B", 20),
                    new Event("A", 3), new Event("B", 30)
            ).gather(keyedTumblingWindow(Event::key, 2))
                    .gather(keyedWindowReduce((Event e1, Event e2) -> new Event(e1.key(), e1.value() + e2.value())))
                    .toList();

            // Full windows only (partial windows are not reduced since they have elements)
            // A: [1,2] → A:3, [3] → A:3
            // B: [10,20] → B:30, [30] → B:30
            assertEquals(4, result.size());
        }
    }

    // ═══════════════════════════════════════════════
    //  Keyed Window Aggregate
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("keyedWindowAggregate")
    class KeyedWindowAggregateTest {

        @Test
        @DisplayName("aggregate per keyed window")
        void aggregatePerKey() {
            List<KeyedResult<String, Double>> result = Stream.of(
                    new Event("A", 10), new Event("A", 20),
                    new Event("B", 100), new Event("B", 200)
            ).gather(keyedTumblingWindow(Event::key, 2))
                    .gather(keyedWindowAggregate(
                            () -> new double[]{0, 0},
                            (acc, e) -> new double[]{acc[0] + e.value(), acc[1] + 1},
                            acc -> acc[0] / acc[1]
                    ))
                    .toList();
            assertEquals(2, result.size());
        }
    }

    // ═══════════════════════════════════════════════
    //  Union
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("union")
    class UnionTest {

        @Test
        @DisplayName("union appends other stream")
        void appendOther() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(union(List.of(4, 5)))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4, 5), result);
        }

        @Test
        @DisplayName("union with empty other")
        void emptyOther() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(union(List.of()))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("union empty stream with other")
        void emptyStreamWithOther() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(union(List.of(1, 2, 3)))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Split / Select
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("split and selectTag")
    class SplitSelectTest {

        @Test
        @DisplayName("split tags elements")
        void splitTags() {
            List<Tagged<Integer>> result = Stream.of(1, 2, 3, 4)
                    .gather(split(x -> x % 2 == 0 ? "even" : "odd"))
                    .toList();
            assertEquals(4, result.size());
            assertEquals("odd", result.get(0).tag());
            assertEquals("even", result.get(1).tag());
        }

        @Test
        @DisplayName("selectTag filters by tag")
        void selectByTag() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6)
                    .gather(split(x -> x % 2 == 0 ? "even" : "odd"))
                    .gather(selectTag("even"))
                    .toList();
            assertEquals(List.of(2, 4, 6), result);
        }

        @Test
        @DisplayName("selectTag with no matches")
        void noMatches() {
            List<Integer> result = Stream.of(1, 3, 5)
                    .gather(split(x -> x % 2 == 0 ? "even" : "odd"))
                    .gather(selectTag("even"))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Connect / CoMap
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("connect and coMap")
    class ConnectCoMapTest {

        @Test
        @DisplayName("connect interleaves with tags")
        void connectWithTags() {
            List<Tagged<Integer>> result = Stream.of(1, 2, 3)
                    .gather(connect(List.of(10, 20)))
                    .toList();
            assertEquals(5, result.size());
            assertEquals(new Tagged<>("main", 1), result.get(0));
            assertEquals(new Tagged<>("other", 10), result.get(1));
            assertEquals(new Tagged<>("main", 2), result.get(2));
            assertEquals(new Tagged<>("other", 20), result.get(3));
            assertEquals(new Tagged<>("main", 3), result.get(4));
        }

        @Test
        @DisplayName("coMap applies different functions per tag")
        void coMapDifferentFunctions() {
            List<String> result = Stream.of(1, 2, 3)
                    .gather(connect(List.of("a", "b")))
                    .gather(coMap(
                            i -> "INT:" + i,
                            s -> "STR:" + s
                    ))
                    .toList();
            assertEquals(List.of("INT:1", "STR:a", "INT:2", "STR:b", "INT:3"), result);
        }

        @Test
        @DisplayName("connect with longer other appends remainder")
        void longerOther() {
            List<Tagged<Integer>> result = Stream.of(1)
                    .gather(connect(List.of(10, 20, 30)))
                    .toList();
            assertEquals(4, result.size());
            assertEquals(new Tagged<>("main", 1), result.get(0));
            assertEquals(new Tagged<>("other", 10), result.get(1));
            assertEquals(new Tagged<>("other", 20), result.get(2));
            assertEquals(new Tagged<>("other", 30), result.get(3));
        }

        @Test
        @DisplayName("coMap with unknown tag throws IllegalArgumentException")
        void coMapUnknownTag() {
            List<Tagged<Integer>> tagged = List.of(new Tagged<>("unknown", 1));
            assertThrows(IllegalArgumentException.class, () ->
                    tagged.stream().gather(coMap(i -> i, i -> i)).toList()
            );
        }
    }

    // ═══════════════════════════════════════════════
    //  Tumbling Time Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("tumblingTimeWindow")
    class TumblingTimeWindowTest {

        @Test
        @DisplayName("align to time boundaries")
        void alignToBoundaries() {
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(10, "a"), new TimedEvent(50, "b"),
                    new TimedEvent(110, "c"), new TimedEvent(150, "d"),
                    new TimedEvent(210, "e")
            ).gather(tumblingTimeWindow(100, TimedEvent::ts))
                    .toList();
            assertEquals(3, result.size());
            // Window 0-99: a(10), b(50)
            assertEquals(2, result.get(0).size());
            assertEquals(0, result.get(0).startIndex());
            assertEquals(99, result.get(0).endIndex());
            // Window 100-199: c(110), d(150)
            assertEquals(2, result.get(1).size());
            assertEquals(100, result.get(1).startIndex());
            assertEquals(199, result.get(1).endIndex());
            // Window 200-299: e(210)
            assertEquals(1, result.get(2).size());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Window<TimedEvent>> result = Stream.<TimedEvent>empty()
                    .gather(tumblingTimeWindow(100, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid size throws")
        void invalidSize() {
            assertThrows(IllegalArgumentException.class, () -> tumblingTimeWindow(0, TimedEvent::ts));
        }
    }

    // ═══════════════════════════════════════════════
    //  Sliding Time Window
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("slidingTimeWindow")
    class SlidingTimeWindowTest {

        @Test
        @DisplayName("overlapping time windows")
        void overlappingWindows() {
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(0, "a"), new TimedEvent(50, "b"),
                    new TimedEvent(100, "c"), new TimedEvent(150, "d")
            ).gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .toList();
            // Aligned starts from floorDiv(0,50)*50 = 0, then 50, 100, 150
            // Window [0,99]: a(0), b(50)
            // Window [50,149]: b(50), c(100)
            // Window [100,199]: c(100), d(150)
            // Window [150,249]: d(150)
            assertEquals(4, result.size());
            assertEquals(0, result.get(0).windowId());
            assertEquals(0, result.get(0).startIndex());
            assertEquals(99, result.get(0).endIndex());
            assertEquals(List.of(new TimedEvent(0, "a"), new TimedEvent(50, "b")), result.get(0).elements());
            assertEquals(2, result.get(0).size()); // a, b
            assertEquals(2, result.get(1).size()); // b, c
            assertEquals(2, result.get(2).size()); // c, d
            assertEquals(1, result.get(3).size()); // d
            assertEquals(150, result.get(3).startIndex());
            assertEquals(249, result.get(3).endIndex());
        }

        @Test
        @DisplayName("invalid params throw")
        void invalidParams() {
            assertThrows(IllegalArgumentException.class, () -> slidingTimeWindow(0, 50, TimedEvent::ts));
            assertThrows(IllegalArgumentException.class, () -> slidingTimeWindow(100, 0, TimedEvent::ts));
        }

        @Test
        @DisplayName("range of timestamps verifies exact window contents and ids")
        void rangeOfTimestamps() {
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(10, "a"), new TimedEvent(60, "b"),
                    new TimedEvent(110, "c"), new TimedEvent(160, "d")
            ).gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .toList();
            assertEquals(4, result.size());

            assertEquals(0, result.get(0).windowId());
            assertEquals(0, result.get(0).startIndex());
            assertEquals(99, result.get(0).endIndex());
            assertEquals(List.of(new TimedEvent(10, "a"), new TimedEvent(60, "b")), result.get(0).elements());

            assertEquals(1, result.get(1).windowId());
            assertEquals(50, result.get(1).startIndex());
            assertEquals(149, result.get(1).endIndex());
            assertEquals(List.of(new TimedEvent(60, "b"), new TimedEvent(110, "c")), result.get(1).elements());

            assertEquals(2, result.get(2).windowId());
            assertEquals(100, result.get(2).startIndex());
            assertEquals(199, result.get(2).endIndex());
            assertEquals(List.of(new TimedEvent(110, "c"), new TimedEvent(160, "d")), result.get(2).elements());

            assertEquals(3, result.get(3).windowId());
            assertEquals(150, result.get(3).startIndex());
            assertEquals(249, result.get(3).endIndex());
            assertEquals(List.of(new TimedEvent(160, "d")), result.get(3).elements());
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Apply
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowApply")
    class WindowApplyTest {

        @Test
        @DisplayName("apply simple function per window")
        void simpleApply() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(tumblingWindow(2))
                    .gather(windowApply(w -> w.elements().stream().mapToInt(i -> i).sum()))
                    .toList();
            assertEquals(List.of(3, 7, 5), result);
        }

        @Test
        @DisplayName("apply on empty window emits function result")
        void applyEmptyWindow() {
            List<String> result = Stream.of(new Window<Integer>(0, 0, 0, List.of()))
                    .gather(windowApply(w -> "empty"))
                    .toList();
            assertEquals(List.of("empty"), result);
        }

        @Test
        @DisplayName("apply propagates exception thrown by function")
        void applyException() {
            Window<Integer> window = new Window<>(0, 0, 2, List.of(1, 2, 3));
            assertThrows(RuntimeException.class, () ->
                    Stream.of(window).gather(windowApply(w -> { throw new RuntimeException("boom"); })).toList()
            );
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Count
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowCount")
    class WindowCountTest {

        @Test
        @DisplayName("count per window")
        void countPerWindow() {
            List<Long> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(tumblingWindow(2))
                    .gather(windowCount())
                    .toList();
            assertEquals(List.of(2L, 2L, 1L), result);
        }

        @Test
        @DisplayName("count on empty window returns zero")
        void countEmptyWindow() {
            List<Long> result = Stream.of(new Window<Integer>(0, 0, 0, List.of()))
                    .gather(windowCount())
                    .toList();
            assertEquals(List.of(0L), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Sum
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowSum")
    class WindowSumTest {

        @Test
        @DisplayName("sum per window")
        void sumPerWindow() {
            List<Double> result = Stream.of(10, 20, 30, 40, 50)
                    .gather(tumblingWindow(2))
                    .gather(windowSum(Integer::doubleValue))
                    .toList();
            assertEquals(List.of(30.0, 70.0, 50.0), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Window Min / Max
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("windowMin and windowMax")
    class WindowMinMaxTest {

        @Test
        @DisplayName("min per window")
        void minPerWindow() {
            List<Optional<Integer>> result = Stream.of(5, 2, 8, 1, 9, 3, 7, 4, 6)
                    .gather(tumblingWindow(3))
                    .gather(windowMin(Comparator.naturalOrder()))
                    .toList();
            assertEquals(List.of(Optional.of(2), Optional.of(1), Optional.of(4)), result);
        }

        @Test
        @DisplayName("max per window")
        void maxPerWindow() {
            List<Optional<Integer>> result = Stream.of(5, 2, 8, 1, 9, 3, 7, 4, 6)
                    .gather(tumblingWindow(3))
                    .gather(windowMax(Comparator.naturalOrder()))
                    .toList();
            assertEquals(List.of(Optional.of(8), Optional.of(9), Optional.of(7)), result);
        }

        @Test
        @DisplayName("min returns empty Optional when minimum element is null")
        void minNull() {
            List<Optional<Integer>> result = Arrays.asList(1, null, 3).stream()
                    .gather(tumblingWindow(3))
                    .gather(windowMin(Comparator.nullsFirst(Comparator.naturalOrder())))
                    .toList();
            assertEquals(List.of(Optional.empty()), result);
        }

        @Test
        @DisplayName("max returns empty Optional when maximum element is null")
        void maxNull() {
            List<Optional<Integer>> result = Arrays.asList(1, null, 3).stream()
                    .gather(tumblingWindow(3))
                    .gather(windowMax(Comparator.nullsLast(Comparator.naturalOrder())))
                    .toList();
            assertEquals(List.of(Optional.empty()), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  End-to-End Pipelines
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("End-to-End Flink Pipelines")
    class PipelineTest {

        @Test
        @DisplayName("keyBy + tumblingWindow + aggregate → per-key average")
        void keyedWindowAverage() {
            List<KeyedResult<String, Double>> result = Stream.of(
                    new Event("sensor-A", 10), new Event("sensor-B", 100),
                    new Event("sensor-A", 20), new Event("sensor-B", 200),
                    new Event("sensor-A", 30), new Event("sensor-B", 300)
            ).gather(keyedTumblingWindow(Event::key, 2))
                    .gather(keyedWindowAggregate(
                            () -> new double[]{0, 0},
                            (acc, e) -> new double[]{acc[0] + e.value(), acc[1] + 1},
                            acc -> acc[0] / acc[1]
                    ))
                    .toList();
            // sensor-A: [10,20] avg=15, [30] avg=30
            // sensor-B: [100,200] avg=150, [300] avg=300
            assertEquals(4, result.size());
        }

        @Test
        @DisplayName("split → selectTag → tumblingWindow → reduce")
        void splitSelectWindowReduce() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6, 7, 8)
                    .gather(split(x -> x % 2 == 0 ? "even" : "odd"))
                    .gather(selectTag("even"))
                    .gather(tumblingWindow(2))
                    .gather(windowReduce(Integer::sum))
                    .toList();
            // evens: 2,4,6,8 → tumblingWindow(2): [2,4],[6,8] → reduce(sum): 6, 14
            assertEquals(List.of(6, 14), result);
        }

        @Test
        @DisplayName("sessionWindow + windowProcess → labeled sessions")
        void sessionWindowProcess() {
            List<String> result = Stream.of(
                    new TimedEvent(1, "click"), new TimedEvent(2, "scroll"),
                    new TimedEvent(50, "click"), new TimedEvent(51, "submit")
            ).gather(sessionWindow(10, TimedEvent::ts))
                    .gather(windowProcess(w -> List.of(
                            "Session#" + w.windowId() + ": " + w.size() + " events"
                    )))
                    .toList();
            assertEquals(List.of("Session#0: 2 events", "Session#1: 2 events"), result);
        }

        @Test
        @DisplayName("tumblingTimeWindow + windowSum → per-interval metric")
        void timeWindowSum() {
            List<Double> result = Stream.of(
                    new TimedEvent(10, "x"), new TimedEvent(50, "x"),
                    new TimedEvent(110, "x"), new TimedEvent(150, "x")
            ).gather(tumblingTimeWindow(100, TimedEvent::ts))
                    .gather(windowCount())
                    .map(Double::valueOf)
                    .toList();
            assertEquals(List.of(2.0, 2.0), result);
        }

        @Test
        @DisplayName("connect + coMap + split + select → complex routing")
        void connectSplitSelect() {
            // Connect two streams, coMap to normalize, then split by type
            List<String> result = Stream.of(1, 2)
                    .gather(connect(List.of("a", "b")))
                    .gather(coMap(
                            i -> "NUM:" + i,
                            s -> "STR:" + s
                    ))
                    .gather(split(s -> s.startsWith("NUM:") ? "numeric" : "text"))
                    .gather(selectTag("text"))
                    .toList();
            assertEquals(List.of("STR:a", "STR:b"), result);
        }
    }

    // ═══════════════════════════════════════════════
    //  Missing branch coverage tests
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("Missing branch coverage")
    class MissingBranchCoverageTest {

        @Test
        @DisplayName("windowReduce with empty window skips it")
        void windowReduceEmptyWindow() {
            // Create an empty Window manually and feed it through windowReduce
            Window<Integer> emptyWindow = new Window<>(0, 0, 0, List.of());
            List<Integer> result = Stream.of(emptyWindow)
                    .gather(windowReduce(Integer::sum))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("windowMin with empty window returns empty")
        void windowMinEmptyWindow() {
            Window<Integer> emptyWindow = new Window<>(0, 0, 0, List.of());
            List<Optional<Integer>> result = Stream.of(emptyWindow)
                    .gather(windowMin(Comparator.naturalOrder()))
                    .toList();
            assertEquals(List.of(Optional.empty()), result);
        }

        @Test
        @DisplayName("windowMax with empty window returns empty")
        void windowMaxEmptyWindow() {
            Window<Integer> emptyWindow = new Window<>(0, 0, 0, List.of());
            List<Optional<Integer>> result = Stream.of(emptyWindow)
                    .gather(windowMax(Comparator.naturalOrder()))
                    .toList();
            assertEquals(List.of(Optional.empty()), result);
        }

        @Test
        @DisplayName("windowProcess with short-circuit (downstream.push returns false)")
        void windowProcessShortCircuit() {
            // windowProcess's for-loop checks downstream.push return
            Window<Integer> window = new Window<>(0, 0, 2, List.of(1, 2, 3));
            List<Integer> result = Stream.of(window)
                    .gather(windowProcess(w -> w.elements()))
                    .limit(1)
                    .toList();
            assertEquals(List.of(1), result);
        }

        @Test
        @DisplayName("keyedWindowReduce with empty window skips it")
        void keyedWindowReduceEmptyWindow() {
            KeyedResult<String, Window<Integer>> emptyKeyed =
                    new KeyedResult<>("key", new Window<>(0, 0, 0, List.of()));
            List<KeyedResult<String, Integer>> result = Stream.of(emptyKeyed)
                    .gather(keyedWindowReduce(Integer::sum))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("keyedTumblingWindow throws for size < 1")
        void keyedTumblingWindowInvalidSize() {
            assertThrows(IllegalArgumentException.class,
                    () -> keyedTumblingWindow(Event::key, 0));
        }

        @Test
        @DisplayName("slidingTimeWindow with startOffset going back (slide < size)")
        void slidingTimeWindowStartOffsetBack() {
            // When slide < size, the startOffset while-loop goes back
            // e.g. size=200, slide=50 → windows overlap significantly
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(100, "a"),
                    new TimedEvent(150, "b"),
                    new TimedEvent(200, "c")
            ).gather(slidingTimeWindow(200, 50, TimedEvent::ts))
                    .toList();
            // At least one window should be produced
            assertFalse(result.isEmpty());
            // All windows should have elements
            for (Window<TimedEvent> w : result) {
                assertFalse(w.isEmpty());
            }
        }

        @Test
        @DisplayName("slidingTimeWindow with some empty windows filtered out")
        void slidingTimeWindowEmptyWindows() {
            // slide=50, size=100: overlapping windows
            // Elements at ts=75, 225 → windows at [0-99](has 75), [50-149](has 75), [100-199](empty), [200-299](has 225), [250-349](has 225)
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(75, "a"),
                    new TimedEvent(225, "b")
            ).gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .toList();
            // The [100-199] window is empty and should be filtered out
            assertFalse(result.isEmpty());
            // All returned windows should be non-empty
            for (Window<TimedEvent> w : result) {
                assertFalse(w.isEmpty());
            }
        }

        @Test
        @DisplayName("slidingTimeWindow with empty stream")
        void slidingTimeWindowEmpty() {
            List<Window<TimedEvent>> result = Stream.<TimedEvent>empty()
                    .gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("connect with limit short-circuits emitOther branch")
        void connectShortCircuit() {
            // When downstream short-circuits, the connect integrator's emitOther push may fail
            List<Tagged<String>> result = Stream.of("a", "b", "c")
                    .gather(connect(List.of("x", "y")))
                    .limit(3)
                    .toList();
            assertEquals(3, result.size());
            assertEquals(new Tagged<>("main", "a"), result.get(0));
        }

        @Test
        @DisplayName("sessionWindow finisher emits buffered elements")
        void sessionWindowFinisherBuffer() {
            // Single element → no gap exceeded → finisher emits the buffer
            List<Window<TimedEvent>> result = Stream.of(
                    new TimedEvent(1, "only")
            ).gather(sessionWindow(10, TimedEvent::ts))
                    .toList();
            assertEquals(1, result.size());
            assertEquals(List.of(new TimedEvent(1, "only")), result.get(0).elements());
        }

        @Test
        @DisplayName("slidingWindow with fewer elements than size produces no output")
        void slidingWindowFewerThanSize() {
            // When count < size, no full window is ever emitted, and the finisher is a no-op
            List<Window<Integer>> result = Stream.of(1, 2)
                    .gather(slidingWindow(3))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════
    //  Short-circuiting with limit(n)
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("short-circuiting")
    class ShortCircuitTest {

        @Test
        @DisplayName("tumblingWindow stops after first emitted window")
        void tumblingWindowLimit() {
            List<Window<Integer>> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(tumblingWindow(2))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
            assertEquals(List.of(0, 1), result.get(0).elements());
        }

        @Test
        @DisplayName("slidingWindow stops after first emitted window")
        void slidingWindowLimit() {
            List<Window<Integer>> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(slidingWindow(3, 2))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
            assertEquals(List.of(0, 1, 2), result.get(0).elements());
        }

        @Test
        @DisplayName("sessionWindow stops after first session")
        void sessionWindowLimit() {
            List<Window<TimedEvent>> result = Stream.iterate(0, i -> i + 10).limit(100)
                    .map(i -> new TimedEvent(i, "v" + i))
                    .gather(sessionWindow(5, TimedEvent::ts))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("tumblingTimeWindow stops after first emitted window")
        void tumblingTimeWindowLimit() {
            List<Window<TimedEvent>> result = Stream.iterate(0, i -> i + 100).limit(100)
                    .map(i -> new TimedEvent(i, "v" + i))
                    .gather(tumblingTimeWindow(100, TimedEvent::ts))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("slidingTimeWindow stops after first emitted window")
        void slidingTimeWindowLimit() {
            List<Window<TimedEvent>> result = Stream.iterate(0, i -> i + 50).limit(100)
                    .map(i -> new TimedEvent(i, "v" + i))
                    .gather(slidingTimeWindow(100, 50, TimedEvent::ts))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("keyedTumblingWindow stops after first emitted keyed window")
        void keyedTumblingWindowLimit() {
            List<KeyedResult<String, Window<Event>>> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .map(i -> new Event("A", i))
                    .gather(keyedTumblingWindow(Event::key, 2))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("keyBy stops after first keyed result")
        void keyByLimit() {
            List<KeyedResult<Integer, String>> result = Stream.of("a", "bb", "c")
                    .gather(keyBy(String::length))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("union stops after limit")
        void unionLimit() {
            List<Integer> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(union(List.of(-1, -2)))
                    .limit(3)
                    .toList();
            assertEquals(List.of(0, 1, 2), result);
        }

        @Test
        @DisplayName("connect stops after limit")
        void connectLimit() {
            List<Tagged<Integer>> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(connect(List.of(-1, -2)))
                    .limit(3)
                    .toList();
            assertEquals(3, result.size());
            assertEquals(new Tagged<>("main", 0), result.get(0));
        }

        @Test
        @DisplayName("split and selectTag stop after limit")
        void splitSelectTagLimit() {
            List<Integer> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(split(i -> i % 2 == 0 ? "even" : "odd"))
                    .gather(selectTag("even"))
                    .limit(2)
                    .toList();
            assertEquals(List.of(0, 2), result);
        }

        @Test
        @DisplayName("windowReduce stops after limit")
        void windowReduceLimit() {
            List<Integer> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(tumblingWindow(2))
                    .gather(windowReduce(Integer::sum))
                    .limit(1)
                    .toList();
            assertEquals(List.of(1), result);
        }

        @Test
        @DisplayName("windowAggregate stops after limit")
        void windowAggregateLimit() {
            List<Integer> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(tumblingWindow(2))
                    .gather(windowAggregate(() -> 0, Integer::sum, i -> i))
                    .limit(1)
                    .toList();
            assertEquals(List.of(1), result);
        }

        @Test
        @DisplayName("windowProcess stops after limit")
        void windowProcessLimit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(tumblingWindow(2))
                    .gather(windowProcess(w -> w.elements()))
                    .limit(2)
                    .toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        @DisplayName("windowApply stops after limit")
        void windowApplyLimit() {
            List<Integer> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(tumblingWindow(2))
                    .gather(windowApply(w -> w.elements().stream().mapToInt(i -> i).sum()))
                    .limit(1)
                    .toList();
            assertEquals(List.of(1), result);
        }

        @Test
        @DisplayName("windowCount stops after limit")
        void windowCountLimit() {
            List<Long> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(tumblingWindow(2))
                    .gather(windowCount())
                    .limit(1)
                    .toList();
            assertEquals(List.of(2L), result);
        }

        @Test
        @DisplayName("windowSum stops after limit")
        void windowSumLimit() {
            List<Double> result = Stream.iterate(0, i -> i + 1).limit(100)
                    .gather(tumblingWindow(2))
                    .gather(windowSum(Integer::doubleValue))
                    .limit(1)
                    .toList();
            assertEquals(List.of(1.0), result);
        }

        @Test
        @DisplayName("windowMin and windowMax stop after limit")
        void windowMinMaxLimit() {
            List<Optional<Integer>> minResult = Stream.of(5, 2, 8, 1, 9, 3)
                    .gather(tumblingWindow(3))
                    .gather(windowMin(Comparator.naturalOrder()))
                    .limit(1)
                    .toList();
            assertEquals(List.of(Optional.of(2)), minResult);
        }
    }

    // ═══════════════════════════════════════════════
    //  Null argument validation
    // ═══════════════════════════════════════════════

    @Nested
    @DisplayName("null argument validation")
    class NullArgumentValidationTest {

        @Test
        @DisplayName("keyBy rejects null extractor")
        void keyByLimit() {
            assertThrows(NullPointerException.class, () -> keyBy((java.util.function.Function<String, Integer>) null));
        }

        @Test
        @DisplayName("sessionWindow rejects null timestamp extractor")
        void sessionWindowLimit() {
            assertThrows(NullPointerException.class, () -> sessionWindow(5, null));
        }

        @Test
        @DisplayName("tumblingTimeWindow rejects null timestamp extractor")
        void tumblingTimeWindowLimit() {
            assertThrows(NullPointerException.class, () -> tumblingTimeWindow(100, null));
        }

        @Test
        @DisplayName("slidingTimeWindow rejects null timestamp extractor")
        void slidingTimeWindowLimit() {
            assertThrows(NullPointerException.class, () -> slidingTimeWindow(100, 50, null));
        }

        @Test
        @DisplayName("windowReduce rejects null reducer")
        void windowReduceLimit() {
            assertThrows(NullPointerException.class, () -> windowReduce(null));
        }

        @Test
        @DisplayName("windowAggregate rejects null functions")
        void windowAggregateLimit() {
            assertThrows(NullPointerException.class, () -> windowAggregate(null, (a, b) -> a, a -> a));
            assertThrows(NullPointerException.class, () -> windowAggregate(() -> 0, null, a -> a));
            assertThrows(NullPointerException.class, () -> windowAggregate(() -> 0, (a, b) -> a, null));
        }

        @Test
        @DisplayName("windowProcess rejects null processor")
        void windowProcessLimit() {
            assertThrows(NullPointerException.class, () -> windowProcess(null));
        }

        @Test
        @DisplayName("windowApply rejects null function")
        void windowApplyLimit() {
            assertThrows(NullPointerException.class, () -> windowApply(null));
        }

        @Test
        @DisplayName("windowSum rejects null extractor")
        void windowSumLimit() {
            assertThrows(NullPointerException.class, () -> windowSum(null));
        }

        @Test
        @DisplayName("windowMin and windowMax reject null comparator")
        void windowMinMaxLimit() {
            assertThrows(NullPointerException.class, () -> windowMin(null));
            assertThrows(NullPointerException.class, () -> windowMax(null));
        }

        @Test
        @DisplayName("split rejects null classifier")
        void splitNull() {
            assertThrows(NullPointerException.class, () -> split(null));
        }

        @Test
        @DisplayName("selectTag rejects null tag")
        void selectTagNull() {
            assertThrows(NullPointerException.class, () -> selectTag(null));
        }

        @Test
        @DisplayName("connect and union reject null iterables")
        void connectUnion() {
            assertThrows(NullPointerException.class, () -> connect(null));
            assertThrows(NullPointerException.class, () -> union(null));
        }

        @Test
        @DisplayName("coMap rejects null mappers")
        void coMapNull() {
            assertThrows(NullPointerException.class, () -> coMap(null, i -> i));
            assertThrows(NullPointerException.class, () -> coMap(i -> i, null));
        }
    }
}
