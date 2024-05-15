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

import static io.crate.metadata.upgrade.IndexTemplateUpgrader.TEMPLATE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.metadata.PartitionName;
import io.crate.server.xcontent.XContentHelper;

public class IndexTemplateUpgraderTest {

    @Test
    public void testDefaultTemplateIsUpgraded() throws IOException {
        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();

        HashMap<String, IndexTemplateMetadata> templates = new HashMap<>();
        IndexTemplateMetadata oldTemplate = IndexTemplateMetadata.builder(TEMPLATE_NAME)
            .patterns(Collections.singletonList("*"))
            .putMapping("{\"default\": {}}")
            .build();
        templates.put(TEMPLATE_NAME, oldTemplate);

        Map<String, IndexTemplateMetadata> upgradedTemplates = upgrader.apply(templates);
        assertThat(upgradedTemplates.get(TEMPLATE_NAME), Matchers.nullValue());
    }

    @Test
    public void testArchivedSettingsAreRemovedOnPartitionedTableTemplates() throws Exception {
        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();

        Settings settings = Settings.builder()
            .put(ARCHIVED_SETTINGS_PREFIX + "some.setting", true)   // archived, must be filtered out
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .build();

        HashMap<String, IndexTemplateMetadata> templates = new HashMap<>();
        String partitionTemplateName = PartitionName.templateName("doc", "t1");
        IndexTemplateMetadata oldPartitionTemplate = IndexTemplateMetadata.builder(partitionTemplateName)
            .settings(settings)
            .putMapping("{\"default\": {}}")
            .patterns(Collections.singletonList("*"))
            .build();
        templates.put(partitionTemplateName, oldPartitionTemplate);

        String nonPartitionTemplateName = "non-partition-template";
        IndexTemplateMetadata oldNonPartitionTemplate = IndexTemplateMetadata.builder(nonPartitionTemplateName)
            .settings(settings)
            .putMapping("{\"default\": {}}")
            .patterns(Collections.singletonList("*"))
            .build();
        templates.put(nonPartitionTemplateName, oldNonPartitionTemplate);

        Map<String, IndexTemplateMetadata> upgradedTemplates = upgrader.apply(templates);
        IndexTemplateMetadata upgradedTemplate = upgradedTemplates.get(partitionTemplateName);
        assertThat(upgradedTemplate.settings().keySet()).containsExactly(SETTING_NUMBER_OF_SHARDS);

        // ensure all other attributes remains the same
        assertThat(upgradedTemplate.mapping()).isEqualTo(oldPartitionTemplate.mapping());
        assertThat(upgradedTemplate.patterns()).isEqualTo(oldPartitionTemplate.patterns());
        assertThat(upgradedTemplate.aliases()).isEqualTo(oldPartitionTemplate.aliases());

        // ensure non partitioned table templates are untouched
        assertThat(upgradedTemplates.get(nonPartitionTemplateName)).isEqualTo(oldNonPartitionTemplate);
    }

    @Test
    public void testInvalidSettingIsRemovedForTemplateInCustomSchema() throws Exception {
        Settings settings = Settings.builder().put("index.recovery.initial_shards", "quorum").build();
        String templateName = PartitionName.templateName("foobar", "t1");
        IndexTemplateMetadata template = IndexTemplateMetadata.builder(templateName)
            .settings(settings)
            .putMapping("{\"default\": {}}")
            .patterns(Collections.singletonList("*"))
            .build();

        IndexTemplateUpgrader indexTemplateUpgrader = new IndexTemplateUpgrader();
        Map<String, IndexTemplateMetadata> result = indexTemplateUpgrader.apply(Collections.singletonMap(templateName, template));

        assertThat(result.get(templateName).settings().hasValue("index.recovery.initial_shards"))
            .as("Outdated setting `index.recovery.initial_shards` must be removed")
            .isFalse();
    }

    @Test
    public void test__all_is_removed_from_template_mapping() throws Throwable {
        String templateName = PartitionName.templateName("doc", "events");
        var template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of("*"))
            .putMapping(
                "{" +
                "   \"default\": {" +
                "       \"_all\": {\"enabled\": false}," +
                "       \"properties\": {" +
                "           \"name\": {" +
                "               \"type\": \"keyword\"" +
                "           }" +
                "       }" +
                "   }" +
                "}")
            .build();

        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();
        Map<String, IndexTemplateMetadata> result = upgrader.apply(Map.of(templateName, template));
        IndexTemplateMetadata updatedTemplate = result.get(templateName);

        CompressedXContent compressedXContent = updatedTemplate.mapping();
        assertThat(compressedXContent.string()).isEqualTo("{\"default\":{\"properties\":{\"name\":{\"position\":1,\"type\":\"keyword\"}}}}");
    }

    @Test
    public void test__dropped_0_is_removed_from_template_mapping() throws Throwable {
        String templateName = PartitionName.templateName("doc", "events");
        var template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of("*"))
            .putMapping(
                "{" +
                    "   \"default\": {" +
                    "       \"properties\": {" +
                    "           \"name\": {" +
                    "               \"type\": \"keyword\"" +
                    "           }," +
                    "           \"_dropped_0\": {" +
                    "           }" +
                    "       }" +
                    "   }" +
                    "}")
            .build();

        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();
        Map<String, IndexTemplateMetadata> result = upgrader.apply(Map.of(templateName, template));
        IndexTemplateMetadata updatedTemplate = result.get(templateName);

        CompressedXContent compressedXContent = updatedTemplate.mapping();
        assertThat(compressedXContent.string()).isEqualTo("{\"default\":{\"properties\":{\"name\":{\"position\":1,\"type\":\"keyword\"}}}}");
    }


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

    @Test
    public void test_copy_to_migrated_to_sources() throws Throwable {
        String templateName = PartitionName.templateName("doc", "events");
        var template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of("*"))
            .putMapping(MappingConstants.FULLTEXT_MAPPING_5_3)
            .build();


        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();
        Map<String, IndexTemplateMetadata> result = upgrader.apply(Map.of(templateName, template));
        IndexTemplateMetadata updatedTemplate = result.get(templateName);

        Map actualMap = XContentHelper.toMap(updatedTemplate.mapping().uncompressed(), XContentType.JSON);
        Map expectedMap = MapperService.parseMapping(NamedXContentRegistry.EMPTY, MappingConstants.FULLTEXT_MAPPING_EXPECTED_IN_5_4);

        assertThat(actualMap)
            .isEqualTo(expectedMap);
    }

    @Test
    public void test_upgrade_deep_nested_object_mapping() throws Exception {
        String templateName = PartitionName.templateName("doc", "events");
        var template = IndexTemplateMetadata.builder(templateName)
            .patterns(List.of("*"))
            .putMapping(MappingConstants.DEEP_NESTED_MAPPING)
            .build();


        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();
        Map<String, IndexTemplateMetadata> result = upgrader.apply(Map.of(templateName, template));
        IndexTemplateMetadata updatedTemplate = result.get(templateName);

        Map actualMap = XContentHelper.toMap(updatedTemplate.mapping().uncompressed(), XContentType.JSON);
        Map expectedMap = MapperService.parseMapping(NamedXContentRegistry.EMPTY, MappingConstants.DEEP_NESTED_MAPPING);

        assertThat(actualMap)
            .isEqualTo(expectedMap);
    }
}
