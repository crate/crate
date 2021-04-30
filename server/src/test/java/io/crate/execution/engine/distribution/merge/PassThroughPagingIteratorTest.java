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

package io.crate.execution.engine.distribution.merge;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

public class PassThroughPagingIteratorTest extends ESTestCase {

    private static <T> PassThroughPagingIterator<Integer, T> iter() {
        return randomBoolean() ? PassThroughPagingIterator.repeatable() : PassThroughPagingIterator.oneShot();
    }

    @Test
    public void testHasNextCallWithoutMerge() throws Exception {
        PassThroughPagingIterator<Integer, Object> iterator = iter();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testInputIsPassedThrough() throws Exception {
        PassThroughPagingIterator<Integer, String> iterator = iter();
        iterator.merge(Arrays.asList(
            new KeyIterable<>(0, Arrays.asList("a", "b", "c")),
            new KeyIterable<>(1, Arrays.asList("d", "e"))));

        iterator.finish();
        ArrayList<String> objects = new ArrayList<>();
        iterator.forEachRemaining(objects::add);
        assertThat(objects, contains("a", "b", "c", "d", "e"));
    }

    @Test
    public void testInputIsPassedThroughWithSecondMergeCall() throws Exception {
        PassThroughPagingIterator<Integer, String> iterator = iter();
        iterator.merge(Arrays.asList(
            new KeyIterable<>(0, Arrays.asList("a", "b", "c")),
            new KeyIterable<>(1, Arrays.asList("d", "e"))));
        iterator.merge(Collections.singletonList(
            new KeyIterable<>(1, Arrays.asList("f", "g"))));
        iterator.finish();
        ArrayList<String> objects = new ArrayList<>();
        iterator.forEachRemaining(objects::add);
        assertThat(objects, contains("a", "b", "c", "d", "e", "f", "g"));
    }
}
