
package org.cratedb.core.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * the regular iterator of a PriorityQueue doesn't guarantee that the elements are returned
 * in the correct order.
 *
 * This iterator wraps a queue and returns the elements in correct order.
 * @param <E>
 */
public class SortedPriorityQueueIterator<E> implements Collection<E>, Iterator<E> {

    private final PriorityQueue<E> priorityQueue;
    private final int limit;
    private int counter;
    private E current;

    public SortedPriorityQueueIterator(PriorityQueue<E> priorityQueue, int limit) {
        this.priorityQueue = priorityQueue;
        this.limit = limit;
        this.counter = 0;
    }

    @Override
    public boolean hasNext() {
        return counter < limit && priorityQueue.peek() != null;
    }

    @Override
    public E next() {
        if (counter >= limit) {
            return null;
        }
        counter++;
        current = priorityQueue.poll();
        return current;
    }

    @Override
    public void remove() {
        priorityQueue.remove(current);
    }

    @Override
    public int size() {
        return Math.min(limit, priorityQueue.size());
    }

    @Override
    public boolean isEmpty() {
        return priorityQueue.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return priorityQueue.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return this;
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> es) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
