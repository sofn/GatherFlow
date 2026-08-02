package com.lesofn.gatherflow.window;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A window of elements with metadata, mirroring Flink's {@code Window} +
 * {@code ProcessWindowFunction.Context}.
 *
 * @param <T> element type
 * @param windowId    sequential window identifier
 * @param startIndex  index of the first element in this window
 * @param endIndex    index of the last element in this window
 * @param elements    elements contained in this window
 */
public record Window<T>(
        long windowId,
        long startIndex,
        long endIndex,
        List<T> elements
) {
    public Window {
        elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    /** Number of elements in this window. */
    public int size() { return elements.size(); }

    /** Whether this window is empty. */
    public boolean isEmpty() { return elements.isEmpty(); }
}
