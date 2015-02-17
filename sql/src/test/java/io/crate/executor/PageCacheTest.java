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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.executor.pageable.CachingPageCache;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class PageCacheTest extends RandomizedTest {


    @Test
    public void testLimit() throws Exception {
        Object[][] source = TestingHelpers.range(0, 100);
        CachingPageCache cache = new CachingPageCache(10);

        cache.put(new PageInfo(0, 10), new ObjectArrayPage(source, 0, 10)); // cached
        cache.put(new PageInfo(10, 10), new ObjectArrayPage(source, 10, 10)); // not cached

        Page result = cache.get(new PageInfo(0, 10));
        assertThat(result, is(notNullValue()));

        result = cache.get(new PageInfo(10, 10));
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testNotFound() throws Exception {
        Object[][] source = TestingHelpers.range(0, 100);
        CachingPageCache cache = new CachingPageCache(10);

        cache.put(new PageInfo(0, 10), new ObjectArrayPage(source, 0, 10));
        Page result = cache.get(new PageInfo(10, 10));
        assertThat(result, is(nullValue()));

        result = cache.get(new PageInfo(100, 10));
        assertThat(result, is(nullValue()));
    }

}
