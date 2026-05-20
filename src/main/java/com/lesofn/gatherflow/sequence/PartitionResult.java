package com.lesofn.gatherflow.sequence;

import java.util.List;

/**
 * Result of a partition operation: two disjoint lists.
 *
 * @param <T> element type
 * @param matching    elements that matched the predicate
 * @param nonMatching elements that did not match the predicate
 */
public record PartitionResult<T>(List<T> matching, List<T> nonMatching) {}
