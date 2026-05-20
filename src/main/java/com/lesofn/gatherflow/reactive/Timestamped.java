package com.lesofn.gatherflow.reactive;

/**
 * A timestamped element — RxJava's {@code Timestamped<T>}, Reactor's timed tuple.
 *
 * @param <T> element type
 * @param timestamp the timestamp value
 * @param value     the element value
 */
public record Timestamped<T>(long timestamp, T value) {}
