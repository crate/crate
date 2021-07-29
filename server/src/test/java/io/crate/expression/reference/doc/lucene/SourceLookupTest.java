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

package io.crate.expression.reference.doc.lucene;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SourceLookupTest {

    @Test
    public void testExtractValueFromNestedObject() {
        Map<String, Map<String, Integer>> map = singletonMap("x", singletonMap("y", 10));
        Object o = SourceLookup.extractValue(map, Arrays.asList("x", "y"), 0);
        assertThat(o, is(10));
    }

    @Test
    public void testExtractValueFromNestedObjectWithinList() {
        Map<String, List<Map<String, Map<String, Integer>>>> m = singletonMap("x", Arrays.asList(
            singletonMap("y", singletonMap("z", 10)),
            singletonMap("y", singletonMap("z", 20))
        ));
        Object o = SourceLookup.extractValue(m, Arrays.asList("x", "y", "z"), 0);
        assertThat((Collection<Integer>) o, contains(is(10), is(20)));
    }

    @Test
    public void testExtractValueFromNestedObjectWithListAsLeaf() {
        Map<String, List<Integer>> m = singletonMap("x", Arrays.asList(10, 20));
        Object o = SourceLookup.extractValue(m, singletonList("x"), 0);
        assertThat((Collection<Integer>) o, contains(is(10), is(20)));
    }
}
