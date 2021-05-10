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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MapComparatorTest extends ESTestCase {

    @Test
    public void testCompareNullMaps() {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("map is null");
        MapComparator.compareMaps(null, null);
    }

    @Test
    public void testCompareMapsWithNullValues() {
        Map<String, Integer> map1 = new HashMap<>() {{
            put("str1", 1);
            put("str2", null);
            put("str3", 3);
        }};
        Map<String, Integer> map2 = new HashMap<>() {{
            put("str1", 1);
            put("str2", 2);
            put("str3", 3);
        }};
        assertThat(MapComparator.compareMaps(map1, map2), is(1));
        assertThat(MapComparator.compareMaps(map2, map1), is(-1));

        map2.put("str2", null);
        assertThat(MapComparator.compareMaps(map2, map1), is(0));
    }

    @Test
    public void testCompareMapsWithValuesOfTheSameClass() {
        Map<String, Integer> map1 = new HashMap<>() {{
            put("str1", 1);
            put("str2", 2);
            put("str3", 3);
        }};
        Map<String, Integer> map2 = new HashMap<>() {{
            put("str1", 1);
            put("str2", 2);
            put("str3", 3);
        }};
        assertThat(MapComparator.compareMaps(map1, map2), is(0));
        assertThat(MapComparator.compareMaps(map2, map1), is(0));

        map2.put("str2", 5);
        assertThat(MapComparator.compareMaps(map1, map2), is(1));
        assertThat(MapComparator.compareMaps(map2, map1), is(1));
    }

    public void testCompareMapsWithValuesOfDifferentClass() {
        Map<String, Number> map1 = new HashMap<>() {{
            put("str1", 1);
            put("str2", 2);
            put("str3", 3);
        }};
        Map<String, Number> map2 = new HashMap<>() {{
            put("str1", 1);
            put("str2", 2L);
            put("str3", 3);
        }};
        assertThat(MapComparator.compareMaps(map1, map2), is(0));
        assertThat(MapComparator.compareMaps(map2, map1), is(0));

        map1.put("str2", 5.0);
        assertThat(MapComparator.compareMaps(map1, map2), is(1));
        assertThat(MapComparator.compareMaps(map2, map1), is(-1));
    }

    public void testCompareMapsWithStringAndBytesRef() {
        /*
         * this can happen when you compare an object with an object literal
         * ... WHERE o = {"x" = 'foo'}
         */
        Map<String, Object> map1 = new HashMap<>() {{
            put("str1", "a");
            put("str3", "3");
        }};
        Map<String, Object> map2 = new HashMap<>() {{
            put("str1", "a");
            put("str3", 3);
        }};
        assertThat(MapComparator.compareMaps(map1, map2), is(0));
        assertThat(MapComparator.compareMaps(map2, map1), is(0));
    }
}
