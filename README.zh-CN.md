# GatherFlow

[English](./README.md)

> **API 稳定性：实验性**
>
> GatherFlow 是一个探索 Java 25 `Stream.gather()` API 的概念验证/实验性库。在发布稳定版本之前，公共 API 可能随时发生变化。

**GatherFlow** 展示了 Java 25 的 `Stream.gather()` API（[JEP 485](https://openjdk.org/jeps/485)），它提供了一系列 Gatherer，用于在**有界、拉取式的 Java Stream** 上模拟 Scala/Vavr 序列、Apache Flink 窗口以及 RxJava/Reactor 响应式算子的**部分**语义。它不能替代 Flink、RxJava 或 Reactor，也不会“忠实复现”它们的运行时行为。

| 模块 | 参考来源 | 描述 |
|---|---|---|
| `sequence` | Scala Collections / Vavr | 函数式序列算子 |
| `window` | Apache Flink | 窗口与带键流算子 |
| `reactive` | RxJava / Project Reactor | 响应式风格的时序与组合算子 |

---

## 项目目标与非目标

**目标**

- 探索 JEP 485 `Gatherer` API 的惯用写法。
- 为内存中的有界流提供确定、可测试的算子。
- 为 `java.util.stream.Stream` 上常见的序列/窗口/响应式风格转换提供统一的词汇表。

**非目标**

- 分布式或并行流处理。
- 无界数据源、事件时间、Watermark 或异步调度。
- 在生产环境中替代 Flink、Kafka Streams、RxJava 或 Project Reactor。
- 精确一次语义、检查点或分布式状态。

> **本库不是**
>
> - **不是**事件时间流处理器（没有 Watermark，没有事件时间）。
> - **没有**检查点、分布式状态或精确一次保证。
> - **不支持**无界数据源或挂钟时间调度。
> - 所有 Gatherer 都是**纯顺序**的；不支持并行流。

---

## 环境要求

- Java 25+（通过 `--enable-preview` 启用预览特性）
- Gradle 8+

---

## 快速开始

所有 Gatherer 工厂方法都是 `SequenceGatherers`、`WindowGatherers` 和 `ReactiveGatherers` 上的 `public static` 方法。使用 `import static ...*` 可以直接在 `Stream.gather(...)` 中调用：

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

使用 Java 25 预览模式编译并运行：

```bash
javac --enable-preview --release 25 -cp gatherflow-*.jar QuickStart.java
java --enable-preview -cp .:gatherflow-*.jar QuickStart
```

在 Gradle 项目中，添加本工程作为依赖，并确保编译与运行任务启用预览特性：

```groovy
tasks.withType(JavaCompile).configureEach {
    options.compilerArgs += ['--enable-preview']
}
tasks.withType(JavaExec).configureEach {
    jvmArgs += ['--enable-preview']
}
```

---

## 构建与测试

```bash
# 运行所有测试并生成覆盖率报告与校验
./gradlew check

# 仅运行测试
./gradlew test

# 生成 JaCoCo HTML 报告
./gradlew jacocoTestReport
```

覆盖率报告输出到 `build/reports/jacoco/`。

---

## 算子参考手册

### `SequenceGatherers` — 来自 Scala / Vavr

| 算子 | 签名 | 描述 | 灵感来源 |
|---|---|---|---|
| `scanLeft` | `(zero, BiFunction<A, T, A>)` | 从左侧累积扫描，输出每一步（含初始值） | Scala `scanLeft` |
| `scanRight` | `(zero, BiFunction<T, A, A>)` | 从右侧累积扫描，缓冲后反向输出；`BiFunction` 接收 `(element, accumulator)` | Scala `scanRight` |
| `sliding` | `(size)` / `(size, step)` | 重叠滑动窗口 | Scala `sliding` |
| `grouped` | `(size)` | 不重叠的固定大小分块 | Scala `grouped` |
| `intersperse` | `(separator)` | 在相邻元素之间插入分隔符 | Vavr / Scala |
| `zipWithIndex` | `()` | 将每个元素与其零基索引配对 | Scala `zipWithIndex` |
| `zip` | `(Iterable)` | 与另一个可迭代对象逐一配对，以较短者为准 | Scala `zip` |
| `distinctBy` | `(keyExtractor)` | 按 key 保留首次出现的元素 | Scala `distinctBy` |
| `takeWhile` | `(predicate)` | 条件成立时持续取元素，首次失败时短路 | Scala `takeWhile` |
| `dropWhile` | `(predicate)` | 条件成立时跳过元素，首次失败后开始转发 | Scala `dropWhile` |
| `partition` | `(predicate)` | 按谓词拆分为两组，末尾一次性输出 | Scala `partition` |
| `flatMap` | `(Function<T, Iterable<R>>)` | 一对多元素展开 | Scala `flatMap` |
| `collect` | `(predicate, mapper)` | 同时过滤与映射 | Scala `collect` |
| `peek` | `(Consumer)` | 旁观副作用，元素原样透传 | Vavr `peek` |
| `prepend` | `(element)` | 先输出该元素，再输出流 | Vavr `prepend` |
| `append` | `(element)` | 先输出流，再输出该元素 | Vavr `append` |
| `cycle` | `(int times)` | 重复元素直到输出总数达到 `times`；`[1,2,3].cycle(7) → [1,2,3,1,2,3,1]` | Scala / Vavr |
| `interleave` | `(Iterable)` | 与另一个可迭代对象交替输出元素 | Vavr `interleave` |
| `reverse` | `()` | 逆序输出所有元素 | Scala `reverse` |
| `slice` | `(fromIndex, toIndex)` | 输出索引范围 [from, to) 内的元素 | Scala `slice` |
| `foldLeft` | `(zero, BiFunction)` | 折叠为单个结果，在流结束时输出 | Scala `foldLeft` |
| `reduceLeft` | `(BinaryOperator)` | 归约为 `Optional` 结果 | Scala `reduceLeft` |
| `groupBy` | `(keyExtractor)` | 分组为 `Map<K, List<T>>`，流结束时输出 | Scala `groupBy` |
| `unfold` | `(seed, Function)` | 通过展开状态生成元素 | Scala `unfold` |

**示例 — scanLeft：**
```java
Stream.of(1, 2, 3, 4, 5)
      .gather(scanLeft(0, Integer::sum))
      .toList();
// [0, 1, 3, 6, 10, 15]
```

**示例 — partition：**
```java
PartitionResult<Integer> p = Stream.of(1, 2, 3, 4, 5)
      .gather(partition(x -> x % 2 == 0))
      .findFirst().orElseThrow();
// p.matching()    -> [2, 4]
// p.nonMatching() -> [1, 3, 5]
```

---

### `WindowGatherers` — 来自 Apache Flink

> 在有界 Java Stream 上模拟 Flink 窗口语义的子集。
> 所有窗口算子输出 [`Window<T>`](src/main/java/com/lesofn/gatherflow/window/Window.java)
> 记录，携带元数据（windowId、startIndex、endIndex、elements）。
> 基于时间的窗口使用元素本地时间戳；不存在事件时间或 Watermark。

#### 窗口算子

| 算子 | 描述 | Flink 对应 |
|---|---|---|
| `tumblingWindow(size)` | 不重叠的计数窗口 | `countWindow(size)` |
| `slidingWindow(size)` / `slidingWindow(size, slide)` | 重叠的计数滑动窗口 | `countWindow(size, slide)` |
| `sessionWindow(gap, tsExtractor)` | 基于间隔的会话窗口 | `EventTimeSessionWindows.withGap` |
| `globalWindow()` | 包含所有元素的单一窗口 | `GlobalWindows.create()` |
| `tumblingTimeWindow(size, tsExtractor)` | 不重叠的时间窗口 | 翻转事件时间窗口 |
| `slidingTimeWindow(size, slide, tsExtractor)` | 重叠的时间滑动窗口 | 滑动事件时间窗口 |

#### 窗口结果算子（接在窗口算子之后）

| 算子 | 描述 | Flink 对应 |
|---|---|---|
| `windowReduce(BinaryOperator)` | 将窗口元素归约为单值 | `WindowedStream.reduce()` |
| `windowAggregate(createAcc, add, getResult)` | 使用独立累加器类型聚合 | `WindowedStream.aggregate()` |
| `windowProcess(Function<Window, Iterable>)` | 完整窗口访问，可输出多个结果 | `WindowedStream.process()` |
| `windowApply(Function<Window<T>, R>)` | 将每个 `Window<T>` 转换为单个结果 | `AllWindowedStream.apply()` |
| `windowCount()` | 输出每个窗口的元素数量 | `WindowedStream.count()` |
| `windowMin(Comparator)` | 每个窗口的最小元素 | — |
| `windowMax(Comparator)` | 每个窗口的最大元素 | — |
| `windowSum(ToDoubleFunction)` | 每个窗口的元素求和 | — |

#### 带键流算子

| 算子 | 描述 | Flink 对应 |
|---|---|---|
| `keyBy(keyExtractor)` | 为每个元素标记键 | `DataStream.keyBy()` |
| `keyedTumblingWindow(keyExtractor, size)` | 按键独立执行翻转窗口 | `keyBy().countWindow(n)` |
| `keyedWindowReduce(BinaryOperator)` | 在每个带键窗口内归约 | `KeyedStream.reduce()` |
| `keyedWindowAggregate(...)` | 在每个带键窗口内聚合 | `KeyedStream.aggregate()` |

#### 路由算子

| 算子 | 描述 | Flink 对应 |
|---|---|---|
| `split(classifier)` | 按字符串标签路由元素 | `DataStream.split()` |
| `selectTag(tag)` | 从 `Tagged<T>` 流中按标签过滤 | `SplitStream.select()` |
| `connect(Iterable)` | 将主流与另一个可迭代对象交错输出，并标记为 `Tagged<T>` 的 `"main"` 与 `"other"`；两侧元素类型相同 | `DataStream.connect()` |
| `coMap(mapMain, mapOther)` | 根据 `Tagged<T>` 的标签应用两个函数之一 | `ConnectedStreams.map()` |
| `union(Iterable)` | 在流后拼接另一个可迭代对象 | `DataStream.union()` |

**可行性汇总：**

| Flink 概念 | Gatherer 支持 | 说明 |
|---|---|---|
| 翻转 / 滑动 / 会话窗口 | 部分支持 | 有界流、元素本地时间戳、无触发器 |
| 全局窗口 | 部分支持 | 有界；在流结束时输出一个窗口，无自定义触发器 |
| KeyBy + 窗口 + Reduce/Aggregate | 完整支持 | 有界流上的按键独立窗口 |
| ProcessWindowFunction | 完整支持 | 可访问完整窗口上下文 |
| Connect / CoMap | 部分支持 | 通过同类型带标签联合类型实现 |
| Split / 侧输出 | 部分支持 | 基于标签的路由 |
| Union | 部分支持 | 在流后拼接另一个可迭代对象，不是真正的并行 union |
| 事件时间 / Watermark | 不可行 | 需要无界推送模型 |
| 检查点 / 精确一次 | 不适用 | 单 JVM，无分布式状态 |

**示例 — 翻转窗口 + 归约：**
```java
Stream.of(1, 2, 3, 4, 5, 6)
      .gather(tumblingWindow(2))
      .gather(windowReduce(Integer::sum))
      .toList();
// [3, 7, 11]
```

**示例 — 带键翻转窗口：**
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

### `ReactiveGatherers` — 来自 RxJava / Project Reactor

> 仅实现 Java `Stream` 中尚未提供的算子。
> 基于时间的算子使用元素内嵌时间戳（确定性），而非挂钟时间。
> 这些 Gatherer 在有界、拉取式的流上模拟 RxJava/Reactor 语义的子集。

#### 时序算子

| 算子 | 描述 | RxJava / Reactor |
|---|---|---|
| `debounce(timeout, tsExtractor)` | 间隔超过阈值时输出最后一个元素 | `debounce` |
| `throttleFirst(windowSize, tsExtractor)` | 每个时间窗口输出第一个元素 | `throttleFirst` / `sampleFirst` |
| `throttleLast(windowSize, tsExtractor)` | 每个时间窗口输出最后一个元素 | `throttleLast` / `sample` |
| `bufferTime(timespan, tsExtractor)` | 将元素按时间桶分批输出 | `buffer(Duration)` |
| `timestamp(tsExtractor)` | 将每个元素包装为带时间戳形式 | `timestamp` |
| `timeInterval(tsExtractor)` | 计算相邻元素间的时间间隔 | `timeInterval` / `elapsed` |
| `delay(tsExtractor)` | 按时间戳重排元素顺序 | `delayElements`（基于排序） |

#### 副作用算子

| 算子 | 描述 | RxJava / Reactor |
|---|---|---|
| `doOnNext(Consumer)` | 每个元素的副作用，原样透传 | `doOnNext` |
| `doOnComplete(Runnable)` | 流正常结束时的副作用 | `doOnComplete` |
| `doOnError(Consumer<Throwable>)` | 观察元素向下游推送时抛出的 `RuntimeException` / `Error`，然后重新抛出；不捕获上游 map/filter 异常 | `doOnError` |
| `doFinally(Consumer<String>)` | 仅在正常完成时触发，接收字符串 `"complete"`；由于 Java Stream 没有错误通道，无法观察错误 | `doFinally` |

#### 错误处理算子

| 算子 | 描述 | RxJava / Reactor |
|---|---|---|
| `onErrorReturn(mapper, fallback)` | 映射函数抛出异常时输出回退值 | `onErrorReturn` |
| `onErrorResume(mapper, fallbackFactory)` | 映射函数抛出异常时切换到回退序列 | `onErrorResume` |
| `retry(mapper, maxRetries)` | 失败时重试映射函数 | `retry` |

#### 流组合算子

| 算子 | 描述 | RxJava / Reactor |
|---|---|---|
| `repeat(times)` | 将缓冲元素重复 N 次 | `repeat` |
| `defaultIfEmpty(value)` | 流为空时输出默认值 | `defaultIfEmpty` |
| `switchIfEmpty(Iterable)` | 流为空时切换到回退序列 | `switchIfEmpty` |
| `startWith(Iterable)` | 在流前追加可迭代序列 | `startWith` |
| `concatWith(Iterable)` | 在流后追加可迭代序列 | `concatWith` |
| `withLatestFrom(other, combiner)` | 对每个主流元素，从 `other` 消费一个值，并与当前见到的最新 `other` 值组合；在 `other` 产生第一个值之前的主流元素会被跳过；若 `other` 比主流短，`other` 的最后一个值会被复用到剩余主流元素 | `withLatestFrom` |

#### 选取算子

| 算子 | 描述 | RxJava / Reactor |
|---|---|---|
| `elementAt(index)` | 输出指定索引的元素，越界则 `Optional.empty()` | `elementAt` |
| `first()` | 输出第一个元素，流为空则 `Optional.empty()` | `first` / `next` |
| `last()` | 输出最后一个元素，流为空则 `Optional.empty()` | `last` |
| `skipLast(n)` | 跳过最后 N 个元素 | `skipLast` |
| `takeLast(n)` | 仅输出最后 N 个元素 | `takeLast` |
| `distinctUntilChanged()` | 抑制相邻重复元素 | `distinctUntilChanged` |
| `distinctUntilChanged(keyExtractor)` | 按 key 抑制相邻重复元素 | `distinctUntilChanged` |

#### 聚合算子

| 算子 | 描述 | RxJava / Reactor |
|---|---|---|
| `scan(seed, accumulator)` | 带种子的扫描（输出包含种子） | `scan` |
| `reduceWith(seed, accumulator)` | 带种子的归约，输出单个结果 | `reduce` |
| `collectList()` | 将所有元素收集为一个 `List` | `collectList` |
| `mapWithIndex(combiner)` | 将每个元素与其索引配对；`BiFunction` 接收 `(Long index, T element)` | `index` |
| `materialize()` | 将元素包装为 `Notification<T>` | `materialize` |
| `dematerialize()` | 解包 `Notification<T>` 流 | `dematerialize` |

**示例 — debounce（防抖）：**
```java
record Event(long ts, String v) {}
Stream.of(new Event(0,"a"), new Event(30,"b"), new Event(100,"c"))
      .gather(debounce(50, Event::ts))
      .toList();
// [Event(30,"b"), Event(100,"c")]
// "b" 被输出，因为间隔(100-30=70) > 50；"c" 在流结束时输出
```

**示例 — distinctUntilChanged（去相邻重复）：**
```java
Stream.of(1, 1, 2, 2, 3, 1, 1)
      .gather(distinctUntilChanged())
      .toList();
// [1, 2, 3, 1]
```

---

## 项目结构

```
src/
  main/java/com/lesofn/gatherflow/
    sequence/
      SequenceGatherers.java      # Scala/Vavr 风格算子
      PartitionResult.java
    window/
      WindowGatherers.java        # Flink 风格算子
      Window.java
      KeyedResult.java
      Tagged.java
    reactive/
      ReactiveGatherers.java      # RxJava/Reactor 风格算子
      Notification.java
      Timed.java
      Timestamped.java
  test/java/com/lesofn/gatherflow/
    sequence/   SequenceGatherersTest.java
    window/     WindowGatherersTest.java
    reactive/   ReactiveGatherersTest.java
    StreamingOperatorTest.java   # 虚拟线程流式测试
```

---

## License

[Apache-2.0](./LICENSE)
