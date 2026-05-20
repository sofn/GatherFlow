package com.lesofn.gatherflow.window;

/**
 * Tagged element for split/connect operations — Flink's Side Output / Connected Streams.
 *
 * @param <T> element type
 * @param tag   the tag identifying the element's source or routing
 * @param value the element value
 */
public record Tagged<T>(String tag, T value) {}
