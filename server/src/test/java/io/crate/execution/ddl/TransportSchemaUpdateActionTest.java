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

package io.crate.execution.ddl;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.crate.metadata.IndexMappings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportSchemaUpdateActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testDynamicTrueCanBeChangedFromBooleanToStringValue() {
        HashMap<String, Object> source = new HashMap<>();
        source.put("dynamic", true);
        TransportSchemaUpdateAction.mergeIntoSource(source, singletonMap("dynamic", "true"));
        assertThat(source.get("dynamic")).isEqualTo("true");
    }

    @Test
    public void testMergeIntoSourceWithNullValuedSource() {
        HashMap<String, Object> source = new HashMap<>();
        source.put("dynamic", null);
        TransportSchemaUpdateAction.mergeIntoSource(source, singletonMap("dynamic", "true"));
        assertThat(source.get("dynamic")).isEqualTo("true");
    }

    @Test
    public void testVersionChangesAreIgnored() {
        HashMap<String, Object> source = new HashMap<>();
        String versionKey = "cratedb";
        source.put(versionKey, 100);

        TransportSchemaUpdateAction.mergeIntoSource(
            source,
            singletonMap(versionKey, 200),
            Arrays.asList("default", "_meta", IndexMappings.VERSION_STRING, versionKey)
        );

        assertThat(source.get(versionKey)).isEqualTo(100);
    }

    @Test
    public void test_populateColumnPositions_method_with_empty_map() {
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(Map.of())).isFalse();
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(Map.of("properties", Map.of()))).isFalse();
    }

    @Test
    public void test_populateColumnPositions_method_without_missing_columns() {
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(
            Map.of("properties",
                   Map.of("a", Map.of("position", 1),
                          "b", Map.of("inner",
                                      Map.of("position", 2,
                                             "properties", Map.of("c", Map.of("position", 3))
                                      )
                       )
                   )
            ))).isFalse();
    }

    @Test
    public void test_populateColumnPositions_method_orders_by_column_order_if_same_level() {
        Map<String, Object> d = new HashMap<>();
        Map<String, Object> e = new HashMap<>();
        // column order
        d.put("position", -2);
        e.put("position", -1);
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(
            Map.of("properties",
                   Map.of("a", Map.of("position", 1,
                                      "properties", Map.of(
                                  "e", e)),
                          "b", Map.of("inner",
                                      Map.of("position", 2,
                                             "properties", Map.of(
                                              "c", Map.of("position", 3),
                                              "d", d)
                                      )
                       )
                   )
            ))).isTrue();
        assertThat(e.get("position")).isEqualTo(4);
        assertThat(d.get("position")).isEqualTo(5);

        // swap d and e
        d = new HashMap<>();
        e = new HashMap<>();
        // column order
        d.put("position", -1);
        e.put("position", -2);
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(
            Map.of("properties",
                   Map.of("a", Map.of("position", 1,
                                      "properties", Map.of(
                                  "d", d)),
                          "b", Map.of("inner",
                                      Map.of("position", 2,
                                             "properties", Map.of(
                                              "c", Map.of("position", 3),
                                              "e", e)
                                      )
                       )
                   )
            ))).isTrue();
        assertThat(d.get("position")).isEqualTo(4);
        assertThat(e.get("position")).isEqualTo(5);
    }

    @Test
    public void test_populateColumnPositions_method_orders_by_level() {
        Map<String, Object> d = new HashMap<>();
        Map<String, Object> f = new HashMap<>();
        d.put("position", -1);
        f.put("position", -2);
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(
            Map.of("properties",
                   Map.of("a", Map.of("position", 1,
                                      "properties", Map.of(
                                  "e", Map.of("position", 4,
                                              "properties", Map.of(
                                          "f", f) // deeper
                                  ))),
                          "b", Map.of("inner",
                                      Map.of("position", 2,
                                             "properties", Map.of(
                                              "c", Map.of("position", 3),
                                              "d", d)
                                      )
                       )
                   )
            ))).isTrue();
        //check d < f
        assertThat(d.get("position")).isEqualTo(5);
        assertThat(f.get("position")).isEqualTo(6);

        // swap d and f
        d = new HashMap<>();
        f = new HashMap<>();
        d.put("position", -1);
        f.put("position", -2);
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(
            Map.of("properties",
                   Map.of("a", Map.of("position", 1,
                                      "properties", Map.of(
                                  "e", Map.of("position", 4,
                                              "properties", Map.of(
                                          "d", d) // deeper
                                  ))),
                          "b", Map.of("inner",
                                      Map.of("position", 2,
                                             "properties", Map.of(
                                              "c", Map.of("position", 3),
                                              "f", f)
                                      )
                       )
                   )
            ))).isTrue();
        // f < d
        assertThat(d.get("position")).isEqualTo(6);
        assertThat(f.get("position")).isEqualTo(5);
    }

    @Test
    public void test_populateColumnPositions_method_ignores_duplicate_column_positions() {
        // duplicates do not break anything
        Map<String, Object> c = new HashMap<>();
        c.put("position", -1);
        Map<String, Object> map = Map.of("properties",
                                         Map.of(
                                             "a", Map.of("position", 1),
                                             "b", Map.of("position", 1),
                                             "c", c
                                         )
        );

        assertThat(TransportSchemaUpdateAction.populateColumnPositions(map)).isTrue();
        assertThat(c.get("position")).isEqualTo(2);
    }

    @Test
    public void test_populateColumnPositions_method_ignores_column_orders() {
        // duplicates do not break anything
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("position", -1);
        b.put("position", -1);
        assertThat(TransportSchemaUpdateAction.populateColumnPositions(Map.of("properties", Map.of("a", a, "b", b)))).isTrue();
        assertThat(a.get("position")).isEqualTo(1);
        assertThat(b.get("position")).isEqualTo(2);
    }
}
