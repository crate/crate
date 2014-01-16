/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
