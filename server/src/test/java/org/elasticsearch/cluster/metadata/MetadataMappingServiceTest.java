/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.cluster.metadata;

import static io.crate.testing.Asserts.assertThrowsMatches;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.is;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MetadataMappingServiceTest {

    @Test
    public void test_populateColumnPositionsImpl_method_with_empty_map() {
        Map<String, Object> map = new HashMap<>();
        MetadataMappingService.populateColumnPositionsImpl(map, map);
        assertTrue(map.isEmpty());
        map.put("properties", new HashMap<>());
        MetadataMappingService.populateColumnPositionsImpl(map, Map.of());
        MetadataMappingService.populateColumnPositionsImpl(map, Map.of("properties", Map.of()));
        assertThat(map, is(Map.of("properties", Map.of())));
    }

    @Test
    public void test_populateColumnPositionsImpl_method_without_missing_columns_still_overrides() {
        Map<String, Object> a = new HashMap<>();
        a.put("position", 1);
        Map<String, Object> indexMapping = Map.of("properties", Map.of("a", a));
        MetadataMappingService.populateColumnPositionsImpl(indexMapping,
                                                           Map.of("properties", Map.of("a", Map.of("position", 10))));
        assertThat(a.get("position"), is(10));
    }

    @Test
    public void test_populateColumnPositionsImpl_method_with_missing_columns_that_is_also_missing_from_template_mapping() {
        Map<String, Object> a = new HashMap<>();
        a.put("position", 1);
        Map<String, Object> indexMapping = Map.of("properties", Map.of("a", a));

        assertThrowsMatches(
            () -> MetadataMappingService.populateColumnPositionsImpl(
                indexMapping,
                Map.of("properties",
                       Map.of("b", Map.of("position", 10)) // template-mapping is missing column 'a'
                )),
                AssertionError.class,
                // template mappings must contain up-to-date and correct column positions that all relevant index mappings can reference.
                "the template mapping is missing column positions"
        );
    }

    @Test
    public void test_populateColumnPositionsImpl_method_with_missing_columns() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> map1 = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        Map<String, Object> map3 = new HashMap<>();
        Map<String, Object> map4 = new HashMap<>();
        Map<String, Object> map5 = new HashMap<>();
        Map<String, Object> map6 = new HashMap<>();
        map.put("properties", map1);
        map1.put("a", map2);
        map2.put("properties", map3);
        map3.put("b", map4);
        map4.put("properties", map5);
        map5.put("d", map6);

        MetadataMappingService.populateColumnPositionsImpl(
            map,
            Map.of("properties", Map.of(
                       "a", Map.of("position", 1,
                                   "properties", Map.of(
                                       "b", Map.of("position", 2,
                                                   "properties", Map.of(
                                                       "d", Map.of("position", 3,
                                                                   "properties", Map.of()))))))));
        assertThat(map2.get("position"), is(1));
        assertThat(map4.get("position"), is(2));
        assertThat(map6.get("position"), is(3));


        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        Map<String, Object> c = new HashMap<>();
        Map<String, Object> d = new HashMap<>();

        a.put("position", 1);
        b.put("position", 2);
        b.put("properties", Map.of("c", c, "d", d));
        c.put("position", 3);

        MetadataMappingService.populateColumnPositionsImpl(
            Map.of("properties",
                  Map.of("a", a,
                         "b", Map.of("inner", b)
                  )),
           Map.of("properties",
                  Map.of("a", Map.of("position", 1),
                         "b", Map.of("inner",
                                     Map.of("position", 2,
                                            "properties", Map.of(
                                             "c", Map.of("position", 3),
                                             "d", Map.of("position", 4))) // to be carried over
                                     )
                      )
                  ));

        assertThat(a.get("position"), is(1));
        assertThat(b.get("position"), is(2));
        assertThat(c.get("position"), is(3));
        assertThat(d.get("position"), is(4));
    }

    @Test
    public void test_populateColumnPositionsImpl_method_overrides_duplicates() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();

        map.put("properties", Map.of("a", a, "b", b));
        a.put("position", 1);
        b.put("position", 1);

        MetadataMappingService.populateColumnPositionsImpl(
            map,
            Map.of("properties", Map.of(
               "a", Map.of("position", 3),
               "b", Map.of("position", 4))));

        assertThat(a.get("position"), is(3));
        assertThat(b.get("position"), is(4));
    }
}
