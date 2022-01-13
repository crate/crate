package io.crate.common.collections;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public class Iterators {

    private Iterators() {
    }

    public static <T> Iterator<T> concat(Iterator<? extends T>... iterators) {
        if (iterators == null) {
            throw new NullPointerException("iterators");
        }
        // explicit generic type argument needed for type inference
        return new ConcatenatedIterator<T>(iterators);
    }

    static class ConcatenatedIterator<T> implements Iterator<T> {
        private final Iterator<? extends T>[] iterators;
        private int index = 0;

        ConcatenatedIterator(Iterator<? extends T>... iterators) {
            if (iterators == null) {
                throw new NullPointerException("iterators");
            }
            for (int i = 0; i < iterators.length; i++) {
                if (iterators[i] == null) {
                    throw new NullPointerException("iterators[" + i + "]");
                }
            }
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = false;
            while (index < iterators.length && !(hasNext = iterators[index].hasNext())) {
                index++;
            }

            return hasNext;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterators[index].next();
        }
    }

    public static <T> Iterator<T> mergeSorted(
        Iterable<? extends Iterator<? extends T>> iterators, Comparator<? super T> comparator) {
        Objects.requireNonNull(iterators, "iterators");
        Objects.requireNonNull(comparator, "comparator");

        return new MergingIterator<T>(iterators, comparator);
    }

    @Nullable
    public static <T> T getNext(Iterator<? extends T> iterator, @Nullable T defaultValue) {
        return iterator.hasNext() ? iterator.next() : defaultValue;
    }

    /**
     * Advances {@code iterator} to the end, returning the last element.
     *
     * @return the last element of {@code iterator}
     * @throws NoSuchElementException if the iterator is empty
     */
    public static <T> T getLast(Iterator<T> iterator) {
        while (true) {
            T current = iterator.next();
            if (!iterator.hasNext()) {
                return current;
            }
        }
    }

    /**
     * Advances {@code iterator} to the end, returning the last element or {@code defaultValue} if the
     * iterator is empty.
     *
     * @param defaultValue the default value to return if the iterator is empty
     * @return the last element of {@code iterator}
     * @since 3.0
     */
    @Nullable
    public static <T> T getLast(Iterator<? extends T> iterator, @Nullable T defaultValue) {
        return iterator.hasNext() ? getLast(iterator) : defaultValue;
    }

    /**
     * An iterator that performs a lazy N-way merge, calculating the next value each time the iterator
     * is polled. This amortizes the sorting cost over the iteration and requires less memory than
     * sorting all elements at once.
     *
     * <p>Retrieving a single element takes approximately O(log(M)) time, where M is the number of
     * iterators. (Retrieving all elements takes approximately O(N*log(M)) time, where N is the total
     * number of elements.)
     */
    private static class MergingIterator<T> implements Iterator<T> {
        final Queue<PeekingIterator<T>> queue;

        public MergingIterator(
            Iterable<? extends Iterator<? extends T>> iterators,
            final Comparator<? super T> itemComparator) {
            // A comparator that's used by the heap, allowing the heap
            // to be sorted based on the top of each iterator.
            Comparator<PeekingIterator<T>> heapComparator =
                new Comparator<PeekingIterator<T>>() {
                    @Override
                    public int compare(PeekingIterator<T> o1, PeekingIterator<T> o2) {
                        return itemComparator.compare(o1.peek(), o2.peek());
                    }
                };

            queue = new PriorityQueue<>(2, heapComparator);

            for (Iterator<? extends T> iterator : iterators) {
                if (iterator.hasNext()) {
                    queue.add(Iterators.peekingIterator(iterator));
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public T next() {
            PeekingIterator<T> nextIter = queue.remove();
            T next = nextIter.next();
            if (nextIter.hasNext()) {
                queue.add(nextIter);
            }
            return next;
        }
    }

    public static <T> Iterator<List<T>> partition(Iterator<T> iterator, int size) {
        return partitionImpl(iterator, size, false);
    }

    private static <T> Iterator<List<T>> partitionImpl(
        final Iterator<T> iterator, final int size, final boolean pad) {
        Objects.requireNonNull(iterator);
        assert(size > 0);
        return new Iterator<List<T>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public List<T> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Object[] array = new Object[size];
                int count = 0;
                for (; count < size && iterator.hasNext(); count++) {
                    array[count] = iterator.next();
                }
                for (int i = count; i < size; i++) {
                    array[i] = null; // for GWT
                }

                @SuppressWarnings("unchecked") // we only put Ts in it
                List<T> list = Collections.unmodifiableList((List<T>) Arrays.asList(array));
                return (pad || count == size) ? list : list.subList(0, count);
            }
        };
    }

    public static <T> PeekingIterator<T> peekingIterator(Iterator<? extends T> iterator) {
        if (iterator instanceof PeekingImpl) {
            // Safe to cast <? extends T> to <T> because PeekingImpl only uses T
            // covariantly (and cannot be subclassed to add non-covariant uses).
            @SuppressWarnings("unchecked")
            PeekingImpl<T> peeking = (PeekingImpl<T>) iterator;
            return peeking;
        }
        return new PeekingImpl<T>(iterator);
    }

    public static <F, T> Iterator<T> transform(
        final Iterator<F> fromIterator, final Function<? super F, ? extends T> function) {
        Objects.requireNonNull(function);
        return new TransformedIterator<F, T>(fromIterator, function);
    }

    private static class TransformedIterator<F, T> implements Iterator<T> {

        final Iterator<? extends F> backingIterator;
        final Function<? super F, ? extends T> function;

        TransformedIterator(Iterator<? extends F> backingIterator, Function<? super F, ? extends T> function) {
            this.backingIterator = Objects.requireNonNull(backingIterator);
            this.function = function;
        }

        public T transform(F from) {
            return function.apply(from);
        }

        @Override
        public final boolean hasNext() {
            return backingIterator.hasNext();
        }

        @Override
        public final T next() {
            return transform(backingIterator.next());
        }

        @Override
        public final void remove() {
            backingIterator.remove();
        }
    }

    private static class PeekingImpl<E> implements PeekingIterator<E> {

        private final Iterator<? extends E> iterator;
        private boolean hasPeeked;
        private @Nullable
        E peekedElement;

        public PeekingImpl(Iterator<? extends E> iterator) {
            this.iterator = Objects.requireNonNull(iterator);
        }

        @Override
        public boolean hasNext() {
            return hasPeeked || iterator.hasNext();
        }

        @Override
        public E next() {
            if (!hasPeeked) {
                return iterator.next();
            }
            E result = peekedElement;
            hasPeeked = false;
            peekedElement = null;
            return result;
        }

        @Override
        public void remove() {
            assert !hasPeeked : "Can't remove after you've peeked at next";
            iterator.remove();
        }

        @Override
        public E peek() {
            if (!hasPeeked) {
                peekedElement = iterator.next();
                hasPeeked = true;
            }
            return peekedElement;
        }
    }

    /**
     * Adds all elements in {@code iterator} to {@code collection}. The iterator will be left
     * exhausted: its {@code hasNext()} method will return {@code false}.
     *
     * @return {@code true} if {@code collection} was modified as a result of this operation
     */
    public static <T> boolean addAll(Collection<T> addTo, Iterator<? extends T> iterator) {
        Objects.requireNonNull(addTo);
        Objects.requireNonNull(iterator);
        boolean wasModified = false;
        while (iterator.hasNext()) {
            wasModified |= addTo.add(iterator.next());
        }
        return wasModified;
    }

}
