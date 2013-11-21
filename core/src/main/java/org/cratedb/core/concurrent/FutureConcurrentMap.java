package org.cratedb.core.concurrent;


import org.cratedb.core.futures.GenericBaseFuture;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureConcurrentMap<K, V> {

    private ConcurrentMap<K, GenericBaseFuture<V>> mapDelegate;

    public static <K, V> FutureConcurrentMap<K, V> newMap() {
        return new FutureConcurrentMap<>();
    }

    FutureConcurrentMap() {
        mapDelegate = ConcurrentCollections.newConcurrentMap();
    }

    public V put(K key, V value) {
        GenericBaseFuture<V> futureValue = new GenericBaseFuture<>();
        GenericBaseFuture<V> previousFutureValue = mapDelegate.putIfAbsent(key, futureValue);
        if (previousFutureValue != null) {
            previousFutureValue.set(value);
        } else {
            futureValue.set(value);
        }

        return value;
    }

    public void remove(K key) {
        mapDelegate.remove(key);
    }

    public V get(K key) throws InterruptedException, ExecutionException, TimeoutException {
        return get(key, 0, TimeUnit.MILLISECONDS);
    }

    public V get(K key, int timeout, TimeUnit timeUnit)
        throws InterruptedException, TimeoutException, ExecutionException
    {
        GenericBaseFuture<V> value = mapDelegate.get(key);
        if (value == null) {
            GenericBaseFuture<V> newFuture = new GenericBaseFuture<>();
            GenericBaseFuture<V> raceCondFuture = mapDelegate.putIfAbsent(key, newFuture);
            if (raceCondFuture != null) {
                return raceCondFuture.get(timeout, timeUnit);
            }

            return newFuture.get(timeout, timeUnit);
        }

        return value.get(timeout, timeUnit);
    }
}
