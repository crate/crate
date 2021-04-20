/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.recycler.Recycler.V;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockPageCacheRecycler extends PageCacheRecycler {

    private static final ConcurrentMap<Object, Throwable> ACQUIRED_PAGES = new ConcurrentHashMap<>();

    public static void ensureAllPagesAreReleased() throws Exception {
        final Map<Object, Throwable> masterCopy = new HashMap<>(ACQUIRED_PAGES);
        if (!masterCopy.isEmpty()) {
            // not empty, we might be executing on a shared cluster that keeps on obtaining
            // and releasing pages, lets make sure that after a reasonable timeout, all master
            // copy (snapshot) have been released
            boolean success =
                    ESTestCase.awaitBusy(() -> Sets.haveEmptyIntersection(masterCopy.keySet(), ACQUIRED_PAGES.keySet()));
            if (!success) {
                masterCopy.keySet().retainAll(ACQUIRED_PAGES.keySet());
                ACQUIRED_PAGES.keySet().removeAll(masterCopy.keySet()); // remove all existing master copy we will report on
                if (!masterCopy.isEmpty()) {
                    Iterator<Throwable> causes = masterCopy.values().iterator();
                    Throwable firstCause = causes.next();
                    RuntimeException exception = new RuntimeException(masterCopy.size() + " pages have not been released", firstCause);
                    while (causes.hasNext()) {
                        exception.addSuppressed(causes.next());
                    }
                    throw exception;
                }
            }
        }
    }

    private final Random random;

    public MockPageCacheRecycler(Settings settings) {
        super(settings);
        // we always initialize with 0 here since we really only wanna have some random bytes / ints / longs
        // and given the fact that it's called concurrently it won't reproduces anyway the same order other than in a unittest
        // for the latter 0 is just fine
        random = new Random(0);
    }

    private <T> V<T> wrap(final V<T> v) {
        ACQUIRED_PAGES.put(v, new Throwable("Unreleased Page from test: " + LuceneTestCase.getTestClass().getName()));
        return new V<T>() {

            @Override
            public void close() {
                final Throwable t = ACQUIRED_PAGES.remove(v);
                if (t == null) {
                    throw new IllegalStateException("Releasing a page that has not been acquired");
                }
                final T ref = v();
                if (ref instanceof Object[]) {
                    Arrays.fill((Object[])ref, 0, Array.getLength(ref), null);
                } else if (ref instanceof byte[]) {
                    Arrays.fill((byte[])ref, 0, Array.getLength(ref), (byte) random.nextInt(256));
                } else if (ref instanceof long[]) {
                    Arrays.fill((long[])ref, 0, Array.getLength(ref), random.nextLong());
                } else if (ref instanceof int[]) {
                    Arrays.fill((int[])ref, 0, Array.getLength(ref), random.nextInt());
                } else if (ref instanceof double[]) {
                    Arrays.fill((double[])ref, 0, Array.getLength(ref), random.nextDouble() - 0.5);
                } else if (ref instanceof float[]) {
                    Arrays.fill((float[])ref, 0, Array.getLength(ref), random.nextFloat() - 0.5f);
                } else {
                    for (int i = 0; i < Array.getLength(ref); ++i) {
                            Array.set(ref, i, (byte) random.nextInt(256));
                    }
                }
                v.close();
            }

            @Override
            public T v() {
                return v.v();
            }

            @Override
            public boolean isRecycled() {
                return v.isRecycled();
            }

        };
    }

    @Override
    public V<byte[]> bytePage(boolean clear) {
        final V<byte[]> page = super.bytePage(clear);
        if (!clear) {
            Arrays.fill(page.v(), 0, page.v().length, (byte)random.nextInt(1<<8));
        }
        return wrap(page);
    }

    @Override
    public V<int[]> intPage(boolean clear) {
        final V<int[]> page = super.intPage(clear);
        if (!clear) {
            Arrays.fill(page.v(), 0, page.v().length, random.nextInt());
        }
        return wrap(page);
    }

    @Override
    public V<long[]> longPage(boolean clear) {
        final V<long[]> page = super.longPage(clear);
        if (!clear) {
            Arrays.fill(page.v(), 0, page.v().length, random.nextLong());
        }
        return wrap(page);
    }

    @Override
    public V<Object[]> objectPage() {
        return wrap(super.objectPage());
    }

}
