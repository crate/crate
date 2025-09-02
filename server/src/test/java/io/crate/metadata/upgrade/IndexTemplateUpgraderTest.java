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

package io.crate.metadata.upgrade;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import io.crate.server.xcontent.LoggingDeprecationHandler;

@SuppressWarnings("deprecation")
public class IndexTemplateUpgraderTest {

    /*
     * test_populateColumnPositions_method_* variants are copied from TransportSchemaUpdateActionTest
     * the only difference is that IndexTemplateUpgrader.populateColumnPositions traverses in Breadth-First order and also resolves duplicates.
     */

    @Test
    public void test_populateColumnPositions_method_with_empty_map() {
        assertThat(IndexTemplateUpgrader.populateColumnPositions(Map.of())).isFalse();
        assertThat(IndexTemplateUpgrader.populateColumnPositions(Map.of("properties", Map.of()))).isFalse();
    }

    @Test
    public void test_populateColumnPositions_method_without_missing_columns() {
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
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
    public void test_populateColumnPositions_method_with_missing_columns() {
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

        assertThat(IndexTemplateUpgrader.populateColumnPositions(map)).isTrue();
        assertThat(map2.get("position")).isEqualTo(1);
        assertThat(map4.get("position")).isEqualTo(2);
        assertThat(map6.get("position")).isEqualTo(3);

        Map<String, Object> d = new HashMap<>();
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
            Map.of("properties",
                   Map.of("a", Map.of("position", 1),
                          "b", Map.of("inner",
                                      Map.of("position", 2,
                                             "properties", Map.of(
                                              "c", Map.of("position", 3),
                                              "d", d)
                                      )
                       )
                   )
            ))).isTrue();
        assertThat(d.get("position")).isEqualTo(4);
    }

    @Test
    public void test_populateColumnPositions_method_with_missing_columns_that_are_same_level_are_order_by_full_path_name() {
        Map<String, Object> d = new HashMap<>();
        Map<String, Object> e = new HashMap<>();
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
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
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
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
    public void test_populateColumnPositions_method_with_missing_columns_order_by_level() {
        Map<String, Object> d = new HashMap<>();
        Map<String, Object> f = new HashMap<>();
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
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
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
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
    public void test_populateColumnPositions_method_groups_columns_under_same_parent() {
        Map<String, Object> p1c = new HashMap<>();
        Map<String, Object> p1cc = new HashMap<>();
        Map<String, Object> p1ccc = new HashMap<>();
        Map<String, Object> p2c = new HashMap<>();
        Map<String, Object> p2cc = new HashMap<>();
        Map<String, Object> p2ccc = new HashMap<>();
        Map<String, Object> p3c = new HashMap<>();
        Map<String, Object> p3cc = new HashMap<>();
        Map<String, Object> p3ccc = new HashMap<>();
        assertThat(IndexTemplateUpgrader.populateColumnPositions(
            Map.of("properties",
                   Map.of("p1", Map.of("position", 3, "properties",
                                       Map.of(
                                           "cc", p1cc,
                                           "c", p1c,
                                           "ccc", p1ccc
                                       )),
                          "p2", Map.of("position", 1, "properties",
                                       Map.of(
                                           "ccc", p2ccc,
                                           "cc", p2cc,
                                           "c", p2c
                                       )),
                          "p3", Map.of("position", 2, "properties",
                                       Map.of(
                                           "ccc", p3ccc,
                                           "c", p3c,
                                           "cc", p3cc
                                       ))
                   )
            )
        )).isTrue();
        assertThat(p1c.get("position")).isEqualTo(4);
        assertThat(p1cc.get("position")).isEqualTo(5);
        assertThat(p1ccc.get("position")).isEqualTo(6);
        assertThat(p2c.get("position")).isEqualTo(7);
        assertThat(p2cc.get("position")).isEqualTo(8);
        assertThat(p2ccc.get("position")).isEqualTo(9);
        assertThat(p3c.get("position")).isEqualTo(10);
        assertThat(p3cc.get("position")).isEqualTo(11);
        assertThat(p3ccc.get("position")).isEqualTo(12);
    }

    @Test
    public void test_populateColumnPositions_method_fixes_duplicates() {

        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        Map<String, Object> c = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        a.put("position", 1);
        b.put("position", 1); // duplicate
        properties.put("a", a);
        properties.put("b", b);
        properties.put("c", c);

        Map<String, Object> map = Map.of("properties", properties);

        assertThat(IndexTemplateUpgrader.populateColumnPositions(map)).isTrue();
        assertThat(a.get("position")).isEqualTo(1);
        assertThat(b.get("position")).isEqualTo(2);
        assertThat(c.get("position")).isEqualTo(3);
    }

    private static Map<String, Object> parse(String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json)) {
            return parser.map();
        }
    }

    @Test
    public void test_copy_to_migrated_to_sources() throws Throwable {
        CompressedXContent mapping = new CompressedXContent(MappingConstants.FULLTEXT_MAPPING_5_3);
        Map<String, Object> mappingSource = IndexTemplateUpgrader.updateMapping(mapping);
        Map<String, Object> expectedMap = parse(MappingConstants.FULLTEXT_MAPPING_EXPECTED_IN_5_4);
        assertThat(mappingSource).isEqualTo(expectedMap);
    }

    @Test
    public void test_upgrade_deep_nested_object_mapping() throws Exception {
        CompressedXContent mapping = new CompressedXContent(MappingConstants.DEEP_NESTED_MAPPING);
        Map<String, Object> mappingSource = IndexTemplateUpgrader.updateMapping(mapping);
        Map<String, Object> expectedMap = parse(MappingConstants.DEEP_NESTED_MAPPING);
        assertThat(mappingSource).isEqualTo(expectedMap);
    }
}
