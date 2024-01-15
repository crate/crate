/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.common.collections;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;


/**
 * Extracted from https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/Iterators.java
 */
public class Iterators {

    private Iterators() {
    }

    /**
     * Combines multiple iterators into a single iterator. The returned iterator iterates across the
     * elements of each iterator in {@code inputs}. The input iterators are not polled until
     * necessary.
     *
     * <p>The returned iterator supports {@code remove()} when the corresponding input iterator
     * supports it. The methods of the returned iterator may throw {@code NullPointerException} if any
     * of the input iterators is null.
     */
    public static <T> Iterator<T> concat(Iterator<? extends Iterator<? extends T>> inputs) {
        return new ConcatenatedIterator<T>(inputs);
    }

    /**
     * Combines multiple iterators into a single iterator. The returned iterator iterates across the
     * elements of each iterator in {@code inputs}. The input iterators are not polled until
     * necessary.
     *
     * <p>The returned iterator supports {@code remove()} when the corresponding input iterator
     * supports it.
     *
     * @throws NullPointerException if any of the provided iterators is null
     */
    @SafeVarargs
    public static final <T extends Object> Iterator<T> concat(Iterator<? extends T>... inputs) {
        return concatNoDefensiveCopy(Arrays.copyOf(inputs, inputs.length));
    }

    /** Concats a varargs array of iterators without making a defensive copy of the array. */
    @SafeVarargs
    static final <T extends Object> Iterator<T> concatNoDefensiveCopy(Iterator<? extends T>... inputs) {
        for (Iterator<? extends T> input : Objects.requireNonNull(inputs)) {
            Objects.requireNonNull(input);
        }
        return concat(consumingForArray(inputs));
    }

    /**
     * Returns an Iterator that walks the specified array, nulling out elements behind it. This can
     * avoid memory leaks when an element is no longer necessary.
     *
     * <p>This method accepts an array with element type {@code @Nullable T}, but callers must pass an
     * array whose contents are initially non-null. The {@code @Nullable} annotation indicates that
     * this method will write nulls into the array during iteration.
     *
     * <p>This is mainly just to avoid the intermediate ArrayDeque in ConsumingQueueIterator.
     */
    @SafeVarargs
    private static final <I extends Iterator<?>> Iterator<I> consumingForArray(@Nullable I... elements) {
        return new Iterator<I>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < elements.length;
            }

            @Override
            public I next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                /*
                 * requireNonNull is safe because our callers always pass non-null arguments. Each element
                 * of the array becomes null only when we iterate past it and then clear it.
                 */
                I result = Objects.requireNonNull(elements[index]);
                elements[index] = null;
                index++;
                return result;
            }
        };
    }

    private static class ConcatenatedIterator<T> implements Iterator<T> {
        /* The last iterator to return an element.  Calls to remove() go to this iterator. */
        private @Nullable Iterator<? extends T> toRemove;

        /* The iterator currently returning elements. */
        private Iterator<? extends T> iterator;

        /*
         * We track the "meta iterators," the iterators-of-iterators, below.  Usually, topMetaIterator
         * is the only one in use, but if we encounter nested concatenations, we start a deque of
         * meta-iterators rather than letting the nesting get arbitrarily deep.  This keeps each
         * operation O(1).
         */

        private Iterator<? extends Iterator<? extends T>> topMetaIterator;

        // Only becomes nonnull if we encounter nested concatenations.
        @Nullable
        private Deque<Iterator<? extends Iterator<? extends T>>> metaIterators;

        ConcatenatedIterator(Iterator<? extends Iterator<? extends T>> metaIterator) {
            iterator = Collections.emptyIterator();
            topMetaIterator = Objects.requireNonNull(metaIterator);
        }

        // Returns a nonempty meta-iterator or, if all meta-iterators are empty, null.
        private @Nullable Iterator<? extends Iterator<? extends T>> getTopMetaIterator() {
            while (topMetaIterator == null || !topMetaIterator.hasNext()) {
                if (metaIterators != null && !metaIterators.isEmpty()) {
                    topMetaIterator = metaIterators.removeFirst();
                } else {
                    return null;
                }
            }
            return topMetaIterator;
        }

        @Override
        public boolean hasNext() {
            while (!Objects.requireNonNull(iterator).hasNext()) {
                // this weird checkNotNull positioning appears required by our tests, which expect
                // both hasNext and next to throw NPE if an input iterator is null.

                topMetaIterator = getTopMetaIterator();
                if (topMetaIterator == null) {
                    return false;
                }

                iterator = topMetaIterator.next();

                if (iterator instanceof ConcatenatedIterator) {
                    // Instead of taking linear time in the number of nested concatenations, unpack
                    // them into the queue
                    @SuppressWarnings("unchecked")
                    ConcatenatedIterator<T> topConcat = (ConcatenatedIterator<T>) iterator;
                    iterator = topConcat.iterator;

                    // topConcat.topMetaIterator, then topConcat.metaIterators, then this.topMetaIterator,
                    // then this.metaIterators

                    if (this.metaIterators == null) {
                        this.metaIterators = new ArrayDeque<>();
                    }
                    this.metaIterators.addFirst(this.topMetaIterator);
                    if (topConcat.metaIterators != null) {
                        while (!topConcat.metaIterators.isEmpty()) {
                            this.metaIterators.addFirst(topConcat.metaIterators.removeLast());
                        }
                    }
                    this.topMetaIterator = topConcat.topMetaIterator;
                }
            }
            return true;
        }

        @Override
        public T next() {
            if (hasNext()) {
                toRemove = iterator;
                return iterator.next();
            } else {
                throw new NoSuchElementException();
            }
        }
    }


    /**
     * Returns an iterator over the merged contents of all given {@code iterators}, traversing every
     * element of the input iterators. Equivalent entries will not be de-duplicated.
     *
     * <p>Callers must ensure that the source {@code iterators} are in non-descending order as this
     * method does not sort its input.
     *
     * <p>For any equivalent elements across all {@code iterators}, it is undefined which element is
     * returned first.
     *
     */
    public static <T> Iterator<T> mergeSorted(
        Iterable<? extends Iterator<? extends T>> iterators, Comparator<? super T> comparator) {
        Objects.requireNonNull(iterators, "iterators");
        Objects.requireNonNull(comparator, "comparator");

        return new MergingIterator<T>(iterators, comparator);
    }

    /**
     * Returns the single element contained in {@code iterator}.
     *
     * @throws NoSuchElementException if the iterator is empty
     * @throws IllegalArgumentException if the iterator contains multiple elements. The state of the
     *     iterator is unspecified.
     */
    public static <T> T getOnlyElement(Iterator<T> iterator) {
        T first = iterator.next();
        if (!iterator.hasNext()) {
            return first;
        }

        StringBuilder sb = new StringBuilder().append("expected one element but was: <").append(first);
        for (int i = 0; i < 4 && iterator.hasNext(); i++) {
            sb.append(", ").append(iterator.next());
        }
        if (iterator.hasNext()) {
            sb.append(", ...");
        }
        sb.append('>');

        throw new IllegalArgumentException(sb.toString());
    }

    /**
     * Returns the next element in {@code iterator} or {@code defaultValue} if the iterator is empty.
     * The {@link Iterables} analog to this method is {@link Iterables#getFirst}.
     *
     * @param defaultValue the default value to return if the iterator is empty
     * @return the next element of {@code iterator} or the default value
     */
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

    /**
     * Divides an iterator into unmodifiable sublists of the given size (the final list may be
     * smaller). For example, partitioning an iterator containing {@code [a, b, c, d, e]} with a
     * partition size of 3 yields {@code [[a, b, c], [d, e]]} -- an outer iterator containing two
     * inner lists of three and two elements, all in the original order.
     *
     * <p>The returned lists implement {@link java.util.RandomAccess}.
     *
     * <p><b>Note:</b> The current implementation eagerly allocates storage for {@code size} elements.
     * As a consequence, passing values like {@code Integer.MAX_VALUE} can lead to {@link
     * OutOfMemoryError}.
     *
     * @param iterator the iterator to return a partitioned view of
     * @param size the desired size of each partition (the last may be smaller)
     * @return an iterator of immutable lists containing the elements of {@code iterator} divided into
     *     partitions
     * @throws IllegalArgumentException if {@code size} is nonpositive
     */
    public static <T> Iterator<List<T>> partition(Iterator<T> iterator, int size) {
        return partitionImpl(iterator, size, false);
    }

    private static <T> Iterator<List<T>> partitionImpl(
        final Iterator<T> iterator, final int size, final boolean pad) {
        Objects.requireNonNull(iterator);
        assert (size > 0);
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
        @Nullable
        private E peekedElement;

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

    static <A, B> Spliterator<B> map(Spliterator<A> fromSpliterator, Function<? super A, ? extends B> function) {
        Objects.requireNonNull(fromSpliterator);
        Objects.requireNonNull(function);
        return new Spliterator<B>() {

            @Override
            public boolean tryAdvance(Consumer<? super B> action) {
                return fromSpliterator.tryAdvance(
                    fromElement -> action.accept(function.apply(fromElement)));
            }

            @Override
            public void forEachRemaining(Consumer<? super B> action) {
                fromSpliterator.forEachRemaining(fromElement -> action.accept(function.apply(fromElement)));
            }

            @Override
            public Spliterator<B> trySplit() {
                Spliterator<A> fromSplit = fromSpliterator.trySplit();
                return (fromSplit != null) ? map(fromSplit, function) : null;
            }

            @Override
            public long estimateSize() {
                return fromSpliterator.estimateSize();
            }

            @Override
            public int characteristics() {
                return fromSpliterator.characteristics()
                    & ~(Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.SORTED);
            }
        };
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

    abstract static class AbstractIndexedListIterator<E> implements ListIterator<E> {
        private final int size;
        private int position;

        /** Returns the element with the specified index. This method is called by {@link #next()}. */
        protected abstract E get(int index);

        /**
         * Constructs an iterator across a sequence of the given size whose initial position is 0. That
         * is, the first call to {@link #next()} will return the first element (or throw {@link
         * NoSuchElementException} if {@code size} is zero).
         *
         * @throws IllegalArgumentException if {@code size} is negative
         */
        protected AbstractIndexedListIterator(int size) {
            this(size, 0);
        }

        /**
         * Constructs an iterator across a sequence of the given size with the given initial position.
         * That is, the first call to {@link #nextIndex()} will return {@code position}, and the first
         * call to {@link #next()} will return the element at that index, if available. Calls to {@link
         * #previous()} can retrieve the preceding {@code position} elements.
         *
         * @throws IndexOutOfBoundsException if {@code position} is negative or is greater than {@code
         *     size}
         * @throws IllegalArgumentException if {@code size} is negative
         */
        protected AbstractIndexedListIterator(int size, int position) {
            Objects.checkIndex(position, size);
            this.size = size;
            this.position = position;
        }

        @Override
        public final boolean hasNext() {
            return position < size;
        }

        @Override
        public final E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return get(position++);
        }

        @Override
        public final int nextIndex() {
            return position;
        }

        @Override
        public final boolean hasPrevious() {
            return position > 0;
        }

        @Override
        public final E previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }
            return get(--position);
        }

        @Override
        public final int previousIndex() {
            return position - 1;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public final void set(E e) {
            throw new UnsupportedOperationException("set");
        }

        @Override
        public final void add(E e) {
            throw new UnsupportedOperationException("add");
        }
    }
}
