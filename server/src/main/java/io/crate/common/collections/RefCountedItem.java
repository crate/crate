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

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.function.Function;


public class RefCountedItem<T> implements AutoCloseable {

    private final Function<String, T> itemFactory;
    private final Consumer<T> closeItem;
    private final ArrayList<String> sources = new ArrayList<>();

    private int refs = 0;
    private T item;

    public RefCountedItem(Function<String, T> itemFactory, Consumer<T> closeItem) {
        this.itemFactory = itemFactory;
        this.closeItem = closeItem;
    }

    public void markAcquired(String source) {
        synchronized (sources) {
            sources.add(source);
            refs++;
            if (item == null) {
                item = itemFactory.apply(source);
            }
        }
    }

    public T item() {
        synchronized (sources) {
            if (item == null) {
                assert !sources.isEmpty() : "Must call `markAcquired` to be able to access the item";
                item = itemFactory.apply(sources.get(sources.size() - 1));
            }
            return item;
        }
    }

    @Override
    public void close() {
        synchronized (sources) {
            refs--;
            if (refs == 0) {
                try {
                    closeItem.accept(item);
                } finally {
                    item = null;
                }
            }
        }
    }
}
