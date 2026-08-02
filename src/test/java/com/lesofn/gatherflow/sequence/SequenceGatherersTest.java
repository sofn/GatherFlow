package com.lesofn.gatherflow.sequence;

import com.lesofn.gatherflow.sequence.PartitionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.lesofn.gatherflow.sequence.SequenceGatherers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for {@link SequenceGatherers}.
 * Each operator is tested with normal, edge, and empty-stream cases.
 */
@DisplayName("SequenceGatherers")
class SequenceGatherersTest {

    private <T> List<T> gather(Stream<T> stream) {
        return stream.toList();
    }

    // ═══════════════════════════════════════════
    //  scanLeft
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("scanLeft")
    class ScanLeftTest {

        @Test
        @DisplayName("cumulative sum includes initial value")
        void cumulativeSum() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(scanLeft(0, Integer::sum))
                    .toList();
            assertEquals(List.of(0, 1, 3, 6, 10, 15), result);
        }

        @Test
        @DisplayName("string concatenation")
        void stringConcat() {
            List<String> result = Stream.of("a", "b", "c")
                    .gather(scanLeft("", (acc, s) -> acc + s))
                    .toList();
            assertEquals(List.of("", "a", "ab", "abc"), result);
        }

        @Test
        @DisplayName("empty stream emits only initial value")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(scanLeft(42, Integer::sum))
                    .toList();
            assertEquals(List.of(42), result);
        }

        @Test
        @DisplayName("single element")
        void singleElement() {
            List<Integer> result = Stream.of(10)
                    .gather(scanLeft(0, Integer::sum))
                    .toList();
            assertEquals(List.of(0, 10), result);
        }
    }

    // ═══════════════════════════════════════════
    //  scanRight
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("scanRight")
    class ScanRightTest {

        @Test
        @DisplayName("cumulative sum from right")
        void cumulativeSumRight() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(scanRight(0, (x, acc) -> x + acc))
                    .toList();
            assertEquals(List.of(6, 5, 3, 0), result);
        }

        @Test
        @DisplayName("empty stream emits only zero")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(scanRight(0, (x, acc) -> x + acc))
                    .toList();
            assertEquals(List.of(0), result);
        }

        @Test
        @DisplayName("string scanRight builds reversed prefix")
        void stringScanRight() {
            List<String> result = Stream.of("a", "b", "c")
                    .gather(scanRight("", (x, acc) -> x + acc))
                    .toList();
            assertEquals(List.of("abc", "bc", "c", ""), result);
        }
    }

    // ═══════════════════════════════════════════
    //  sliding
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("sliding")
    class SlidingTest {

        @Test
        @DisplayName("sliding window size 3")
        void slidingSize3() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(sliding(3))
                    .toList();
            assertEquals(List.of(
                    List.of(1, 2, 3),
                    List.of(2, 3, 4),
                    List.of(3, 4, 5)
            ), result);
        }

        @Test
        @DisplayName("sliding window with step 2")
        void slidingWithStep() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(sliding(3, 2))
                    .toList();
            assertEquals(List.of(
                    List.of(1, 2, 3),
                    List.of(3, 4, 5)
            ), result);
        }

        @Test
        @DisplayName("sliding window larger than stream emits partial")
        void windowLargerThanStream() {
            List<List<Integer>> result = Stream.of(1, 2)
                    .gather(sliding(5))
                    .toList();
            assertEquals(List.of(List.of(1, 2)), result);
        }

        @Test
        @DisplayName("empty stream produces no windows")
        void emptyStream() {
            List<List<Integer>> result = Stream.<Integer>empty()
                    .gather(sliding(3))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("size 1 produces single-element windows")
        void sizeOne() {
            List<List<Integer>> result = Stream.of(1, 2, 3)
                    .gather(sliding(1))
                    .toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        @DisplayName("invalid size throws")
        void invalidSize() {
            assertThrows(IllegalArgumentException.class, () -> sliding(0));
            assertThrows(IllegalArgumentException.class, () -> sliding(-1));
        }

        @Test
        @DisplayName("invalid step throws")
        void invalidStep() {
            assertThrows(IllegalArgumentException.class, () -> sliding(2, 0));
        }

        @Test
        @DisplayName("step 2 with trailing partial window")
        void stepTwoTrailingPartial() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4)
                    .gather(sliding(3, 2))
                    .toList();
            assertEquals(List.of(
                    List.of(1, 2, 3),
                    List.of(3, 4)
            ), result);
        }

        @Test
        @DisplayName("step 2 trailing partial after multiple full windows")
        void stepTwoTrailingPartialAfterFullWindows() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4, 5, 6)
                    .gather(sliding(3, 2))
                    .toList();
            assertEquals(List.of(
                    List.of(1, 2, 3),
                    List.of(3, 4, 5),
                    List.of(5, 6)
            ), result);
        }

        @Test
        @DisplayName("large input sliding performance")
        void largeInputPerformance() {
            assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                    IntStream.range(0, 1_000_000).boxed()
                            .gather(sliding(1000))
                            .skip(999_000)
                            .limit(1)
                            .toList()
            );
        }
    }

    // ═══════════════════════════════════════════
    //  grouped
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("grouped")
    class GroupedTest {

        @Test
        @DisplayName("group into pairs with remainder")
        void pairsWithRemainder() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(grouped(2))
                    .toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), result);
        }

        @Test
        @DisplayName("exact fit produces no remainder")
        void exactFit() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4)
                    .gather(grouped(2))
                    .toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), result);
        }

        @Test
        @DisplayName("empty stream produces no groups")
        void emptyStream() {
            List<List<Integer>> result = Stream.<Integer>empty()
                    .gather(grouped(2))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("group size 1 wraps each element")
        void sizeOne() {
            List<List<Integer>> result = Stream.of(1, 2, 3)
                    .gather(grouped(1))
                    .toList();
            assertEquals(List.of(List.of(1), List.of(2), List.of(3)), result);
        }

        @Test
        @DisplayName("invalid size throws")
        void invalidSize() {
            assertThrows(IllegalArgumentException.class, () -> grouped(0));
        }

        @Test
        @DisplayName("large input grouped performance")
        void largeInputPerformance() {
            assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                    IntStream.range(0, 1_000_000).boxed()
                            .gather(grouped(1000))
                            .skip(999)
                            .limit(1)
                            .toList()
            );
        }
    }

    // ═══════════════════════════════════════════
    //  intersperse
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("intersperse")
    class IntersperseTest {

        @Test
        @DisplayName("intersperse zero between numbers")
        void numbersWithZero() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(intersperse(0))
                    .toList();
            assertEquals(List.of(1, 0, 2, 0, 3), result);
        }

        @Test
        @DisplayName("intersperse comma between strings")
        void stringsWithComma() {
            List<String> result = Stream.of("a", "b", "c")
                    .gather(intersperse(","))
                    .toList();
            assertEquals(List.of("a", ",", "b", ",", "c"), result);
        }

        @Test
        @DisplayName("single element produces no separator")
        void singleElement() {
            List<Integer> result = Stream.of(42)
                    .gather(intersperse(0))
                    .toList();
            assertEquals(List.of(42), result);
        }

        @Test
        @DisplayName("empty stream produces nothing")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(intersperse(0))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  zipWithIndex
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("zipWithIndex")
    class ZipWithIndexTest {

        @Test
        @DisplayName("strings with index")
        void stringsWithIndex() {
            List<Map.Entry<String, Long>> result = Stream.of("a", "b", "c")
                    .gather(zipWithIndex())
                    .toList();
            assertEquals(3, result.size());
            assertEquals("a", result.get(0).getKey());
            assertEquals(0L, result.get(0).getValue());
            assertEquals("c", result.get(2).getKey());
            assertEquals(2L, result.get(2).getValue());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Map.Entry<Object, Long>> result = Stream.empty()
                    .gather(zipWithIndex())
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  distinctBy
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("distinctBy")
    class DistinctByTest {

        @Test
        @DisplayName("distinct by string length")
        void distinctByLength() {
            List<String> result = Stream.of("aa", "bb", "ab", "ccc")
                    .gather(distinctBy(String::length))
                    .toList();
            assertEquals(List.of("aa", "ccc"), result);
        }

        @Test
        @DisplayName("distinct by first character")
        void distinctByFirstChar() {
            List<String> result = Stream.of("apple", "banana", "avocado", "cherry")
                    .gather(distinctBy(s -> s.charAt(0)))
                    .toList();
            assertEquals(List.of("apple", "banana", "cherry"), result);
        }

        @Test
        @DisplayName("all unique keys keeps all")
        void allUnique() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(distinctBy(i -> i))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(distinctBy(i -> i))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  takeWhile
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("takeWhile")
    class TakeWhileTest {

        @Test
        @DisplayName("take while less than 3")
        void takeWhileLessThan3() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 1, 2)
                    .gather(takeWhile(x -> x < 3))
                    .toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        @DisplayName("all match takes all")
        void allMatch() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(takeWhile(x -> x < 10))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("none match takes none")
        void noneMatch() {
            List<Integer> result = Stream.of(5, 6, 7)
                    .gather(takeWhile(x -> x < 3))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(takeWhile(x -> true))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  dropWhile
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("dropWhile")
    class DropWhileTest {

        @Test
        @DisplayName("drop while less than 3")
        void dropWhileLessThan3() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 1, 2)
                    .gather(dropWhile(x -> x < 3))
                    .toList();
            assertEquals(List.of(3, 4, 1, 2), result);
        }

        @Test
        @DisplayName("all match drops all")
        void allMatch() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(dropWhile(x -> x < 10))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("none match drops none")
        void noneMatch() {
            List<Integer> result = Stream.of(5, 6, 7)
                    .gather(dropWhile(x -> x < 3))
                    .toList();
            assertEquals(List.of(5, 6, 7), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(dropWhile(x -> true))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  partition
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("partition")
    class PartitionTest {

        @Test
        @DisplayName("even and odd")
        void evenOdd() {
            PartitionResult<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(partition(x -> x % 2 == 0))
                    .findFirst().orElseThrow();
            assertEquals(List.of(2, 4), result.matching());
            assertEquals(List.of(1, 3, 5), result.nonMatching());
        }

        @Test
        @DisplayName("all matching")
        void allMatching() {
            PartitionResult<Integer> result = Stream.of(2, 4, 6)
                    .gather(partition(x -> x % 2 == 0))
                    .findFirst().orElseThrow();
            assertEquals(List.of(2, 4, 6), result.matching());
            assertTrue(result.nonMatching().isEmpty());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            PartitionResult<Integer> result = Stream.<Integer>empty()
                    .gather(partition(x -> true))
                    .findFirst().orElseThrow();
            assertTrue(result.matching().isEmpty());
            assertTrue(result.nonMatching().isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  flatMap
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("flatMap")
    class FlatMapTest {

        @Test
        @DisplayName("expand each element to pair")
        void expandToPair() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(flatMap(x -> List.of(x, x * 10)))
                    .toList();
            assertEquals(List.of(1, 10, 2, 20, 3, 30), result);
        }

        @Test
        @DisplayName("filter via empty iterable")
        void filterViaEmpty() {
            List<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(flatMap(x -> x % 2 == 0 ? List.of(x) : List.of()))
                    .toList();
            assertEquals(List.of(2, 4), result);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(flatMap(x -> List.of(x, x)))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  collect (filter + map)
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("collect")
    class CollectTest {

        @Test
        @DisplayName("even numbers multiplied by 10")
        void evenMultiplied() {
            List<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(collect(x -> x % 2 == 0, x -> x * 10))
                    .toList();
            assertEquals(List.of(20, 40), result);
        }

        @Test
        @DisplayName("none match")
        void noneMatch() {
            List<Integer> result = Stream.of(1, 3, 5)
                    .gather(collect(x -> x % 2 == 0, x -> x * 10))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(collect(x -> true, x -> x))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  peek
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("peek")
    class PeekTest {

        @Test
        @DisplayName("peek observes without consuming")
        void peekObserves() {
            List<Integer> sideEffects = new ArrayList<>();
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(peek(sideEffects::add))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
            assertEquals(List.of(1, 2, 3), sideEffects);
        }

        @Test
        @DisplayName("peek works mid-chain")
        void peekMidChain() {
            List<Integer> beforeMap = new ArrayList<>();
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(peek(beforeMap::add))
                    .map(x -> x * 2)
                    .toList();
            assertEquals(List.of(2, 4, 6), result);
            assertEquals(List.of(1, 2, 3), beforeMap);
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> sideEffects = new ArrayList<>();
            List<Integer> result = Stream.<Integer>empty()
                    .gather(peek(sideEffects::add))
                    .toList();
            assertTrue(result.isEmpty());
            assertTrue(sideEffects.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  prepend
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("prepend")
    class PrependTest {

        @Test
        @DisplayName("prepend element before stream")
        void prependElement() {
            List<Integer> result = Stream.of(2, 3)
                    .gather(prepend(1))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("prepend to empty stream emits the element")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(prepend(1))
                    .toList();
            assertEquals(List.of(1), result);
        }
    }

    // ═══════════════════════════════════════════
    //  append
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("append")
    class AppendTest {

        @Test
        @DisplayName("append element after stream")
        void appendElement() {
            List<Integer> result = Stream.of(1, 2)
                    .gather(append(3))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("append to empty stream emits the element")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(append(1))
                    .toList();
            assertEquals(List.of(1), result);
        }
    }

    // ═══════════════════════════════════════════
    //  cycle
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("cycle")
    class CycleTest {

        @Test
        @DisplayName("cycle 7 elements from 3-element source")
        void cycle7() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(cycle(7))
                    .toList();
            assertEquals(List.of(1, 2, 3, 1, 2, 3, 1), result);
        }

        @Test
        @DisplayName("cycle 0 produces nothing")
        void cycle0() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(cycle(0))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("cycle exact multiple")
        void cycleExactMultiple() {
            List<Integer> result = Stream.of(1, 2)
                    .gather(cycle(6))
                    .toList();
            assertEquals(List.of(1, 2, 1, 2, 1, 2), result);
        }

        @Test
        @DisplayName("cycle empty stream produces nothing")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(cycle(5))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("negative times throws")
        void negativeTimes() {
            assertThrows(IllegalArgumentException.class, () -> cycle(-1));
        }
    }

    // ═══════════════════════════════════════════
    //  interleave
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("interleave")
    class InterleaveTest {

        @Test
        @DisplayName("interleave two lists")
        void interleaveTwo() {
            List<Integer> result = Stream.of(1, 3, 5)
                    .gather(interleave(List.of(2, 4)))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4, 5), result);
        }

        @Test
        @DisplayName("interleave with longer other appends remainder")
        void longerOther() {
            List<Integer> result = Stream.of(1)
                    .gather(interleave(List.of(2, 3, 4)))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4), result);
        }

        @Test
        @DisplayName("interleave with empty other is identity")
        void emptyOther() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(interleave(List.of()))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("interleave empty stream with other emits other")
        void emptyStreamWithOther() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(interleave(List.of(1, 2, 3)))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }
    }

    // ═══════════════════════════════════════════
    //  foldLeft
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("foldLeft")
    class FoldLeftTest {

        @Test
        @DisplayName("sum of numbers")
        void sum() {
            Integer result = Stream.of(1, 2, 3)
                    .gather(foldLeft(0, Integer::sum))
                    .findFirst().orElseThrow();
            assertEquals(6, result);
        }

        @Test
        @DisplayName("string concatenation")
        void stringConcat() {
            String result = Stream.of("a", "b", "c")
                    .gather(foldLeft("", (acc, s) -> acc + s))
                    .findFirst().orElseThrow();
            assertEquals("abc", result);
        }

        @Test
        @DisplayName("empty stream returns initial value")
        void emptyStream() {
            Integer result = Stream.<Integer>empty()
                    .gather(foldLeft(42, Integer::sum))
                    .findFirst().orElseThrow();
            assertEquals(42, result);
        }
    }

    // ═══════════════════════════════════════════
    //  reduceLeft
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("reduceLeft")
    class ReduceLeftTest {

        @Test
        @DisplayName("product of numbers")
        void product() {
            Optional<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(reduceLeft((a, b) -> a * b))
                    .findFirst().orElseThrow();
            assertEquals(Optional.of(24), result);
        }

        @Test
        @DisplayName("empty stream returns empty optional")
        void emptyStream() {
            Optional<Integer> result = Stream.<Integer>empty()
                    .gather(reduceLeft(Integer::sum))
                    .findFirst().orElseThrow();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("single element returns that element")
        void singleElement() {
            Optional<Integer> result = Stream.of(42)
                    .gather(reduceLeft(Integer::sum))
                    .findFirst().orElseThrow();
            assertEquals(Optional.of(42), result);
        }

        @Test
        @DisplayName("null reduced result produces empty optional")
        void nullResult() {
            Optional<Integer> result = Stream.of(1, 2)
                    .gather(reduceLeft((a, b) -> null))
                    .findFirst().orElseThrow();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  reverse
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("reverse")
    class ReverseTest {

        @Test
        @DisplayName("reverse integers")
        void reverseInts() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(reverse())
                    .toList();
            assertEquals(List.of(3, 2, 1), result);
        }

        @Test
        @DisplayName("reverse single element")
        void singleElement() {
            List<Integer> result = Stream.of(1)
                    .gather(reverse())
                    .toList();
            assertEquals(List.of(1), result);
        }

        @Test
        @DisplayName("reverse empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(reverse())
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  slice
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("slice")
    class SliceTest {

        @Test
        @DisplayName("slice from 1 to 4")
        void slice1to4() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(slice(1, 4))
                    .toList();
            assertEquals(List.of(2, 3, 4), result);
        }

        @Test
        @DisplayName("slice from 0 to 3")
        void slice0to3() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(slice(0, 3))
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("slice beyond stream end")
        void sliceBeyondEnd() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(slice(1, 10))
                    .toList();
            assertEquals(List.of(2, 3), result);
        }

        @Test
        @DisplayName("empty range")
        void emptyRange() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(slice(2, 2))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("empty stream")
        void emptyStream() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(slice(0, 3))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("invalid range throws")
        void invalidRange() {
            assertThrows(IllegalArgumentException.class, () -> slice(-1, 2));
            assertThrows(IllegalArgumentException.class, () -> slice(3, 1));
        }
    }

    // ═══════════════════════════════════════════
    //  zip
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("zip")
    class ZipTest {

        @Test
        @DisplayName("zip with shorter other")
        void zipWithShorter() {
            List<Map.Entry<Integer, String>> result = Stream.of(1, 2, 3)
                    .gather(zip(List.of("a", "b")))
                    .toList();
            assertEquals(2, result.size());
            assertEquals(1, result.get(0).getKey());
            assertEquals("a", result.get(0).getValue());
            assertEquals(2, result.get(1).getKey());
            assertEquals("b", result.get(1).getValue());
        }

        @Test
        @DisplayName("zip with longer other stops at stream end")
        void zipWithLonger() {
            List<Map.Entry<Integer, String>> result = Stream.of(1)
                    .gather(zip(List.of("a", "b", "c")))
                    .toList();
            assertEquals(1, result.size());
            assertEquals(1, result.get(0).getKey());
            assertEquals("a", result.get(0).getValue());
        }

        @Test
        @DisplayName("zip with empty other produces nothing")
        void emptyOther() {
            List<Map.Entry<Integer, String>> result = Stream.of(1, 2, 3)
                    .gather(zip(List.<String>of()))
                    .toList();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("zip empty stream produces nothing")
        void emptyStream() {
            List<Map.Entry<Integer, String>> result = Stream.<Integer>empty()
                    .gather(zip(List.of("a", "b")))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  unfold
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("unfold")
    class UnfoldTest {

        @Test
        @DisplayName("unfold counting from 1 to 5")
        void counting() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(unfold(1, s -> s <= 5
                            ? Optional.of(new AbstractMap.SimpleImmutableEntry<Integer, Integer>(s, s + 1))
                            : Optional.empty()))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4, 5), result);
        }

        @Test
        @DisplayName("unfold fibonacci")
        void fibonacci() {
            List<Long> result = Stream.<Long>empty()
                    .gather(unfold(
                            new long[]{0, 1},
                            pair -> pair[0] <= 50
                                    ? Optional.of(new AbstractMap.SimpleImmutableEntry<Long, long[]>(pair[0], new long[]{pair[1], pair[0] + pair[1]}))
                                    : Optional.empty()))
                    .toList();
            assertEquals(List.of(0L, 1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L), result);
        }

        @Test
        @DisplayName("unfold with immediate empty produces nothing")
        void immediateEmpty() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(unfold(1, (Integer s) -> Optional.<Map.Entry<Integer, Integer>>empty()))
                    .toList();
            assertTrue(result.isEmpty());
        }
    }

    // ═══════════════════════════════════════════
    //  groupBy
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("groupBy")
    class GroupByTest {

        @Test
        @DisplayName("group strings by length")
        void groupByLength() {
            Map<Integer, List<String>> result = Stream.of("aa", "bb", "ab", "ccc")
                    .gather(groupBy(String::length))
                    .findFirst().orElseThrow();
            assertEquals(2, result.size());
            assertEquals(List.of("aa", "bb", "ab"), result.get(2));
            assertEquals(List.of("ccc"), result.get(3));
        }

        @Test
        @DisplayName("group integers by parity")
        void groupByParity() {
            Map<String, List<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(groupBy(x -> x % 2 == 0 ? "even" : "odd"))
                    .findFirst().orElseThrow();
            assertEquals(List.of(2, 4), result.get("even"));
            assertEquals(List.of(1, 3, 5), result.get("odd"));
        }

        @Test
        @DisplayName("empty stream produces empty map")
        void emptyStream() {
            Map<Integer, List<String>> result = Stream.<String>empty()
                    .gather(groupBy(String::length))
                    .findFirst().orElseThrow();
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("result map is immutable")
        void immutableResult() {
            Map<Integer, List<String>> result = Stream.of("aa", "bb", "ccc")
                    .gather(groupBy(String::length))
                    .findFirst().orElseThrow();
            assertThrows(UnsupportedOperationException.class, () -> result.put(1, List.of()));
        }
    }

    // ═══════════════════════════════════════════
    //  Short-circuiting with limit
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("short-circuiting with limit")
    class ShortCircuitingTest {

        @Test
        @DisplayName("scanLeft")
        void scanLeftShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(scanLeft(0, Integer::sum))
                    .limit(3)
                    .toList();
            assertEquals(List.of(0, 1, 3), result);
        }

        @Test
        @DisplayName("scanRight")
        void scanRightShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(scanRight(0, Integer::sum))
                    .limit(3)
                    .toList();
            assertEquals(List.of(15, 14, 12), result);
        }

        @Test
        @DisplayName("sliding")
        void slidingShortCircuit() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(sliding(2))
                    .limit(3)
                    .toList();
            assertEquals(List.of(List.of(1, 2), List.of(2, 3), List.of(3, 4)), result);
        }

        @Test
        @DisplayName("grouped")
        void groupedShortCircuit() {
            List<List<Integer>> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(grouped(2))
                    .limit(2)
                    .toList();
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), result);
        }

        @Test
        @DisplayName("zipWithIndex")
        void zipWithIndexShortCircuit() {
            List<Map.Entry<String, Long>> result = Stream.of("a", "b", "c", "d")
                    .gather(zipWithIndex())
                    .limit(2)
                    .toList();
            assertEquals(2, result.size());
            assertEquals("a", result.get(0).getKey());
            assertEquals(0L, result.get(0).getValue());
            assertEquals("b", result.get(1).getKey());
            assertEquals(1L, result.get(1).getValue());
        }

        @Test
        @DisplayName("slice")
        void sliceShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6)
                    .gather(slice(1, 5))
                    .limit(2)
                    .toList();
            assertEquals(List.of(2, 3), result);
        }

        @Test
        @DisplayName("interleave")
        void interleaveShortCircuit() {
            List<Integer> result = Stream.of(1, 3, 5, 7)
                    .gather(interleave(List.of(2, 4, 6)))
                    .limit(5)
                    .toList();
            assertEquals(List.of(1, 2, 3, 4, 5), result);
        }

        @Test
        @DisplayName("reverse")
        void reverseShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(reverse())
                    .limit(2)
                    .toList();
            assertEquals(List.of(5, 4), result);
        }

        @Test
        @DisplayName("cycle")
        void cycleShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(cycle(10))
                    .limit(4)
                    .toList();
            assertEquals(List.of(1, 2, 3, 1), result);
        }

        @Test
        @DisplayName("intersperse")
        void intersperseShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(intersperse(0))
                    .limit(3)
                    .toList();
            assertEquals(List.of(1, 0, 2), result);
        }

        @Test
        @DisplayName("prepend")
        void prependShortCircuit() {
            List<Integer> result = Stream.of(2, 3)
                    .gather(prepend(1))
                    .limit(2)
                    .toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        @DisplayName("append")
        void appendShortCircuit() {
            List<Integer> result = Stream.of(1, 2)
                    .gather(append(3))
                    .limit(2)
                    .toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        @DisplayName("distinctBy")
        void distinctByShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 1, 2)
                    .gather(distinctBy(i -> i))
                    .limit(3)
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("collect")
        void collectShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4)
                    .gather(collect(x -> x % 2 == 0, x -> x * 10))
                    .limit(1)
                    .toList();
            assertEquals(List.of(20), result);
        }

        @Test
        @DisplayName("flatMap")
        void flatMapShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(flatMap(x -> List.of(x, x * 10)))
                    .limit(3)
                    .toList();
            assertEquals(List.of(1, 10, 2), result);
        }

        @Test
        @DisplayName("takeWhile")
        void takeWhileShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(takeWhile(x -> x < 5))
                    .limit(2)
                    .toList();
            assertEquals(List.of(1, 2), result);
        }

        @Test
        @DisplayName("dropWhile")
        void dropWhileShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(dropWhile(x -> x < 3))
                    .limit(2)
                    .toList();
            assertEquals(List.of(3, 4), result);
        }

        @Test
        @DisplayName("partition")
        void partitionShortCircuit() {
            PartitionResult<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(partition(x -> x % 2 == 0))
                    .limit(1)
                    .findFirst()
                    .orElseThrow();
            assertEquals(List.of(2, 4), result.matching());
        }

        @Test
        @DisplayName("zip")
        void zipShortCircuit() {
            List<Map.Entry<Integer, String>> result = Stream.of(1, 2, 3, 4)
                    .gather(zip(List.of("a", "b")))
                    .limit(1)
                    .toList();
            assertEquals(1, result.size());
            assertEquals(1, result.get(0).getKey());
            assertEquals("a", result.get(0).getValue());
        }

        @Test
        @DisplayName("unfold")
        void unfoldShortCircuit() {
            List<Integer> result = Stream.<Integer>empty()
                    .gather(SequenceGatherers.<Integer, Integer>unfold(1, s -> s <= 100
                            ? Optional.of(Map.entry(s, s + 1))
                            : Optional.empty()))
                    .limit(3)
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }

        @Test
        @DisplayName("groupBy")
        void groupByShortCircuit() {
            Map<Integer, List<String>> result = Stream.of("aa", "bb", "ab", "ccc")
                    .gather(groupBy(String::length))
                    .limit(1)
                    .findFirst()
                    .orElseThrow();
            assertEquals(List.of("aa", "bb", "ab"), result.get(2));
        }

        @Test
        @DisplayName("foldLeft")
        void foldLeftShortCircuit() {
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(foldLeft(0, Integer::sum))
                    .limit(1)
                    .toList();
            assertEquals(List.of(6), result);
        }

        @Test
        @DisplayName("reduceLeft")
        void reduceLeftShortCircuit() {
            List<Optional<Integer>> result = Stream.of(1, 2, 3)
                    .gather(reduceLeft(Integer::sum))
                    .limit(1)
                    .toList();
            assertEquals(List.of(Optional.of(6)), result);
        }
    }

    // ═══════════════════════════════════════════
    //  Null argument validation
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("null argument validation")
    class NullArgumentValidationTest {

        @Test
        @DisplayName("scanLeft")
        void scanLeftNullOperator() {
            assertThrows(NullPointerException.class,
                    () -> scanLeft(0, (BiFunction<Integer, Integer, Integer>) null));
        }

        @Test
        @DisplayName("scanRight")
        void scanRightNullOperator() {
            assertThrows(NullPointerException.class,
                    () -> scanRight(0, (BiFunction<Integer, Integer, Integer>) null));
        }

        @Test
        @DisplayName("distinctBy")
        void distinctByNullExtractor() {
            assertThrows(NullPointerException.class,
                    () -> distinctBy((Function<String, Integer>) null));
        }

        @Test
        @DisplayName("takeWhile")
        void takeWhileNullPredicate() {
            assertThrows(NullPointerException.class,
                    () -> takeWhile((Predicate<Integer>) null));
        }

        @Test
        @DisplayName("dropWhile")
        void dropWhileNullPredicate() {
            assertThrows(NullPointerException.class,
                    () -> dropWhile((Predicate<Integer>) null));
        }

        @Test
        @DisplayName("partition")
        void partitionNullPredicate() {
            assertThrows(NullPointerException.class,
                    () -> partition((Predicate<Integer>) null));
        }

        @Test
        @DisplayName("flatMap")
        void flatMapNullMapper() {
            assertThrows(NullPointerException.class,
                    () -> flatMap((Function<Integer, Iterable<Integer>>) null));
        }

        @Test
        @DisplayName("collect")
        void collectNullArgs() {
            assertThrows(NullPointerException.class,
                    () -> collect((Predicate<Integer>) null, (Function<Integer, Integer>) null));
        }

        @Test
        @DisplayName("peek")
        void peekNullAction() {
            assertThrows(NullPointerException.class,
                    () -> peek((Consumer<Integer>) null));
        }

        @Test
        @DisplayName("interleave")
        void interleaveNullIterable() {
            assertThrows(NullPointerException.class,
                    () -> interleave((Iterable<Integer>) null));
        }

        @Test
        @DisplayName("foldLeft")
        void foldLeftNullOperator() {
            assertThrows(NullPointerException.class,
                    () -> foldLeft(0, (BiFunction<Integer, Integer, Integer>) null));
        }

        @Test
        @DisplayName("reduceLeft")
        void reduceLeftNullOperator() {
            assertThrows(NullPointerException.class,
                    () -> reduceLeft((BinaryOperator<Integer>) null));
        }

        @Test
        @DisplayName("zip")
        void zipNullIterable() {
            assertThrows(NullPointerException.class,
                    () -> zip((Iterable<String>) null));
        }

        @Test
        @DisplayName("unfold")
        void unfoldNullFunction() {
            assertThrows(NullPointerException.class,
                    () -> unfold(1, (Function<Integer, Optional<Map.Entry<Integer, Integer>>>) null));
        }

        @Test
        @DisplayName("groupBy")
        void groupByNullExtractor() {
            assertThrows(NullPointerException.class,
                    () -> groupBy((Function<String, Integer>) null));
        }
    }

    // ═══════════════════════════════════════════
    //  Chaining / Composition
    // ═══════════════════════════════════════════

    @Nested
    @DisplayName("Chaining and composition")
    class ChainingTest {

        @Test
        @DisplayName("prepend + append forms complete range")
        void prependAppend() {
            List<Integer> result = Stream.of(2, 3, 4)
                    .gather(prepend(1))
                    .gather(append(5))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4, 5), result);
        }

        @Test
        @DisplayName("takeWhile + map")
        void takeWhileMap() {
            List<Integer> result = Stream.of(1, 2, 3, 10, 4, 5)
                    .gather(takeWhile(x -> x < 5))
                    .map(x -> x * 10)
                    .toList();
            assertEquals(List.of(10, 20, 30), result);
        }

        @Test
        @DisplayName("sliding + foldLeft per window")
        void slidingFold() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(sliding(3))
                    .map(window -> window.stream().mapToInt(i -> i).sum())
                    .toList();
            assertEquals(List.of(6, 9, 12), result);
        }

        @Test
        @DisplayName("distinctBy + intersperse + collect")
        void distinctByIntersperseCollect() {
            // "apple"(5), "ant"(3), "banana"(6), "berry"(5=dup), "cherry"(6=dup)
            List<String> result = Stream.of("apple", "ant", "banana", "berry", "cherry")
                    .gather(distinctBy(String::length))
                    .gather(intersperse("|"))
                    .toList();
            assertEquals(List.of("apple", "|", "ant", "|", "banana"), result);
        }

        @Test
        @DisplayName("grouped + flatMap flattens chunks")
        void groupedFlatMap() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5, 6)
                    .gather(grouped(2))
                    .gather(flatMap(chunk -> chunk.reversed()))
                    .toList();
            assertEquals(List.of(2, 1, 4, 3, 6, 5), result);
        }

        @Test
        @DisplayName("zipWithIndex + filter by index")
        void zipWithIndexFilter() {
            List<String> result = Stream.of("a", "b", "c", "d", "e")
                    .gather(zipWithIndex())
                    .filter(e -> e.getValue() % 2 == 0)
                    .map(Map.Entry::getKey)
                    .toList();
            assertEquals(List.of("a", "c", "e"), result);
        }

        @Test
        @DisplayName("reverse + takeWhile")
        void reverseTakeWhile() {
            List<Integer> result = Stream.of(1, 2, 3, 4, 5)
                    .gather(reverse())
                    .gather(takeWhile(x -> x > 2))
                    .toList();
            assertEquals(List.of(5, 4, 3), result);
        }

        @Test
        @DisplayName("flatMap with limit short-circuits downstream.push returning false")
        void flatMapShortCircuit() {
            // flatMap's for-loop checks downstream.push return; combining with limit
            // causes downstream.push to return false after enough elements
            List<Integer> result = Stream.of(1, 2, 3)
                    .gather(flatMap(x -> List.of(x, x * 10)))
                    .limit(3)
                    .toList();
            assertEquals(List.of(1, 10, 2), result);
        }

        @Test
        @DisplayName("interleave with limit short-circuits emitOther branch")
        void interleaveShortCircuit() {
            // When downstream short-circuits, the interleave integrator should stop
            List<Integer> result = Stream.of(1, 3, 5, 7)
                    .gather(interleave(List.of(2, 4, 6)))
                    .limit(4)
                    .toList();
            assertEquals(List.of(1, 2, 3, 4), result);
        }

        @Test
        @DisplayName("unfold with non-empty input stream (integrator ignores input)")
        void unfoldWithNonEmptyInput() {
            // The integrator lambda (unused, element, downstream) -> true ignores input
            // and the finisher generates values. Feed non-empty input to cover this branch.
            List<Integer> result = Stream.of(99, 100, 101)
                    .gather(SequenceGatherers.<Integer, Integer>unfold(1, s -> s <= 5 ? Optional.of(Map.entry(s, s + 1)) : Optional.empty()))
                    .toList();
            assertEquals(List.of(1, 2, 3, 4, 5), result);
        }

        @Test
        @DisplayName("unfold with limit short-circuits downstream.push returning false")
        void unfoldShortCircuit() {
            // The finisher's !downstream.push() branch: combine unfold with limit
            List<Integer> result = Stream.<Integer>empty()
                    .gather(SequenceGatherers.<Integer, Integer>unfold(1, s -> s <= 100 ? Optional.of(Map.entry(s, s + 1)) : Optional.empty()))
                    .limit(3)
                    .toList();
            assertEquals(List.of(1, 2, 3), result);
        }
    }
}
