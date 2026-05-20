package com.lesofn.gatherflow.reactive;

import java.util.Objects;

/**
 * A stream notification — materialized form of onNext/onError/onComplete.
 * RxJava: {@code Notification<T>}, Reactor: {@code Signal<T>}.
 */
public sealed interface Notification<T> {
    /** An element notification (onNext). */
    record OnNext<T>(T value) implements Notification<T> {
        public OnNext { Objects.requireNonNull(value); }
    }
    /** An error notification (onError). */
    record OnError<T>(Throwable error) implements Notification<T> {
        public OnError { Objects.requireNonNull(error); }
    }
    /** A completion notification (onComplete). */
    record OnComplete<T>() implements Notification<T> {}
}
