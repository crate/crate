/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.core.collections;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * an ArrayBlockingQueue that only contains unique elements
 * @param <T> type of the element
 */
public class UniqueBlockingQueue<T> {

    private static enum PlaceHolder { DUMMY }
    private final ConcurrentHashMap<T, PlaceHolder> queuedIndices;
    private final BlockingQueue<T> requestQueue;

    public UniqueBlockingQueue(int capacity) {
        this.queuedIndices = new ConcurrentHashMap<>(capacity);
        this.requestQueue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * puts element to the queue if it is not already in the queue
     * @param element the thing to add
     * @return true if element was putted
     */
    public boolean put(T element) throws InterruptedException {
        PlaceHolder previous = queuedIndices.putIfAbsent(element, PlaceHolder.DUMMY);
        if (previous == null) {
            this.requestQueue.put(element);
        }
        return previous == null; // added
    }

    public T take() throws InterruptedException {
        T element = requestQueue.take();
        queuedIndices.remove(element);
        return element;
    }
}
