package com.lesofn.gatherflow.reactive;

/**
 * A timed interval element — Reactor's {@code Timed<T>} with elapsed time.
 *
 * @param <T> element type
 * @param elapsedMillis elapsed time in milliseconds since the previous element
 * @param value         the element value
 */
public record Timed<T>(long elapsedMillis, T value) {}
