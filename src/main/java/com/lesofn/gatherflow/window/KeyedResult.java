package com.lesofn.gatherflow.window;

/**
 * Result of a keyed window operation, pairing the key with the window result.
 *
 * @param <K> key type
 * @param <R> result type
 * @param key    the grouping key
 * @param result the computed result for this key
 */
public record KeyedResult<K, R>(K key, R result) {}
