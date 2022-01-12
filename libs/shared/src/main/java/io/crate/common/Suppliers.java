package io.crate.common;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/*
 * Extracted from https://github.com/google/guava/blob/master/guava/src/com/google/common/base/Suppliers.java
 */
public class Suppliers {

    private Suppliers() {}

    public static class MemoizingSupplier<T> implements Supplier<T>, Serializable {
        final Supplier<T> delegate;
        transient volatile boolean initialized;
        // "value" does not need to be volatile; visibility piggy-backs
        // on volatile read of "initialized".
        transient @Nullable T value;

        public MemoizingSupplier(Supplier<T> delegate) {
            assert delegate != null;
            this.delegate = delegate;
        }

        @Override
        public T get() {
            // A 2-field variant of Double Checked Locking.
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        T t = delegate.get();
                        value = t;
                        initialized = true;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override
        public String toString() {
            return "Suppliers.memoize("
                + (initialized ? "<supplier that returned " + value + ">" : delegate)
                + ")";
        }

        private static final long serialVersionUID = 0;
    }

    public static class ExpiringMemoizingSupplier<T> implements Supplier<T>, Serializable {
        final Supplier<T> delegate;
        final long durationNanos;
        transient volatile @Nullable
        T value;
        // The special value 0 means "not yet initialized".
        transient volatile long expirationNanos;

        public ExpiringMemoizingSupplier(Supplier<T> delegate, long duration, TimeUnit unit) {
            assert delegate != null;
            this.delegate = delegate;
            assert (duration > 0) : String.format("duration (%s %s) must be > 0", duration, unit);
            this.durationNanos = unit.toNanos(duration);
        }

        @Override
        public T get() {
            // Another variant of Double Checked Locking.
            //
            // We use two volatile reads. We could reduce this to one by
            // putting our fields into a holder class, but (at least on x86)
            // the extra memory consumption and indirection are more
            // expensive than the extra volatile reads.
            long nanos = expirationNanos;
            long now = System.nanoTime();
            if (nanos == 0 || now - nanos >= 0) {
                synchronized (this) {
                    if (nanos == expirationNanos) { // recheck for lost race
                        T t = delegate.get();
                        value = t;
                        nanos = now + durationNanos;
                        // In the very unlikely event that nanos is 0, set it to 1;
                        // no one will notice 1 ns of tardiness.
                        expirationNanos = (nanos == 0) ? 1 : nanos;
                        return t;
                    }
                }
            }
            return value;
        }

        @Override
        public String toString() {
            // This is a little strange if the unit the user provided was not NANOS,
            // but we don't want to store the unit just for toString
            return "Suppliers.memoizeWithExpiration(" + delegate + ", " + durationNanos + ", NANOS)";
        }

        private static final long serialVersionUID = 0;
    }

}
