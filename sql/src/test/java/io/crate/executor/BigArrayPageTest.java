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

package io.crate.executor;

import com.google.common.collect.Iterators;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BigArrayPageTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private BigArrays bigArrays;

    @Before
    public void prepare() {
        PageCacheRecycler pageCacheRecycler = new PageCacheRecycler(ImmutableSettings.EMPTY, new ThreadPool(getClass().getName()));
        bigArrays = new BigArrays(ImmutableSettings.EMPTY, pageCacheRecycler, null);
    }

    @Test
    public void testEmptyArray() throws Exception {
        BigArrayPage page = new BigArrayPage(bigArrays.<Object[]>newObjectArray(0), 0, 0);
        assertThat(page.size(), is(0L));
        assertThat(page.iterator().hasNext(), is(false));

        page = new BigArrayPage(bigArrays.<Object[]>newObjectArray(0), 0, 10);
        assertThat(page.size(), is(0L));
        assertThat(page.iterator().hasNext(), is(false));
    }

    @Test
    public void testBigArray() throws Exception {
        ObjectArray<Object[]> pageSource = bigArrays.newObjectArray(100);
        for (long i = 0; i < pageSource.size(); i++) {
            pageSource.set(i, new Object[]{ i });
        }
        BigArrayPage page = new BigArrayPage(pageSource, 0, 10);
        assertThat(page.size(), is(10L));
        assertThat(Iterators.size(page.iterator()), is(10));

        page = new BigArrayPage(pageSource, 10, 10);
        assertThat(page.size(), is(10L));
        assertThat(Iterators.size(page.iterator()), is(10));

        page = new BigArrayPage(pageSource, 0, 100);
        assertThat(page.size(), is(100L));
        assertThat(Iterators.size(page.iterator()), is(100));

        page = new BigArrayPage(pageSource, 10, 100);
        assertThat(page.size(), is(90L));
        assertThat(Iterators.size(page.iterator()), is(90));

        page = new BigArrayPage(pageSource, 10, 101);
        assertThat(page.size(), is(90L));
        assertThat(Iterators.size(page.iterator()), is(90));

        page = new BigArrayPage(pageSource);
        assertThat(page.size(), is(100L));
        assertThat(Iterators.size(page.iterator()), is(100));
    }

    @Test
    public void testExceed() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("start exceeds page");
        new BigArrayPage(bigArrays.<Object[]>newObjectArray(2), 3, 1);
    }
}
