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

package io.crate.core;

import java.util.concurrent.TimeUnit;

/**
 * A cache for a single reference that is cached for <code>cacheTime</code> milliseconds
 * and after that refreshed by calling {@link #refresh()} on new access to {@link #get()}.
 * @param <T>
 */
public abstract class CachedRef<T> {

    private final long cacheTime;
    private long cachedAt = 0L;
    private volatile T value = null;

    public CachedRef(long cacheTime, TimeUnit timeUnit) {
        this.cacheTime = timeUnit.toMillis(cacheTime);
    }

    /**
     * guaranteed to be not null if {@link #refresh()} does not return null.
     * @return the cached value or a refreshed one.
     */
    public T get() {
        ensureValue();
        return value;
    }

    private synchronized void ensureValue() {
        long curTime = System.currentTimeMillis();
        if (value == null || curTime - cachedAt > cacheTime) {
            value = refresh();
            cachedAt = curTime;
        }
    }

    /**
     * create a new value
     * @return the new value
     */
    protected abstract T refresh();
}
