package org.cratedb.core.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that starts at the beginning when end of iterated collection is reached
 * @param <T>
 */
public class CyclicIterator<T> implements Iterator<T> {
    private final Collection<T> c;
    private Iterator<T> it;

    public CyclicIterator(Collection<T> c) {
        this.c = c;
        this.it = this.c.iterator();
    }

    public boolean hasNext() {
        return !c.isEmpty();
    }

    public T next() {
        T ret;

        if (!hasNext()) {
            throw new NoSuchElementException();
        } else if (it.hasNext()) {
            ret = it.next();
        } else {
            it = c.iterator();
            ret = it.next();
        }

        return ret;
    }

    public void remove() {
        it.remove();
    }
}
