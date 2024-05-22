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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.Constants;
import io.crate.metadata.RelationName;

public class MetadataIndexUpgraderTest extends ESTestCase {



    @Test
    public void testDynamicStringTemplateIsPurged() throws IOException {
        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        MappingMetadata mappingMetadata = new MappingMetadata(createDynamicStringMappingTemplate());
        MappingMetadata newMappingMetadata = metadataIndexUpgrader.createUpdatedIndexMetadata(mappingMetadata, "dummy", null);

        Object dynamicTemplates = newMappingMetadata.sourceAsMap().get("dynamic_templates");
        assertThat(dynamicTemplates).isNull();

        // Check that the new metadata still has the root "default" element
        assertThat("{\"default\":{}}").isEqualTo(newMappingMetadata.source().toString());
    }

    @Test
    public void test__all_is_removed_from_mapping() throws Throwable {
        IndexMetadata indexMetadata = IndexMetadata.builder(new RelationName("doc", "users").indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(
                "{" +
                "   \"_all\": {\"enabled\": false}," +
                "   \"properties\": {" +
                "       \"name\": {" +
                "           \"type\": \"keyword\"" +
                "       }" +
                "   }" +
                "}")
            .build();

        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        IndexMetadata updatedMetadata = metadataIndexUpgrader.apply(indexMetadata, null);

        MappingMetadata mapping = updatedMetadata.mapping();
        assertThat(mapping.source().string(), Matchers.is("{\"default\":{\"properties\":{\"name\":{\"type\":\"keyword\",\"position\":1}}}}"));
    }

    @Test
    public void test__dropped_0_is_removed_from_mapping() throws Throwable {
        IndexMetadata indexMetadata = IndexMetadata.builder(new RelationName("doc", "users").indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(
                "{" +
                    "   \"_all\": {\"enabled\": false}," +
                    "   \"properties\": {" +
                    "       \"name\": {" +
                    "           \"type\": \"keyword\"" +
                    "       }," +
                    "       \"_dropped_0\": {" +
                    "       }" +
                    "   }" +
                    "}")
            .build();

        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        IndexMetadata updatedMetadata = metadataIndexUpgrader.apply(indexMetadata, null);

        MappingMetadata mapping = updatedMetadata.mapping();
        assertThat(mapping.source().string(), Matchers.is("{\"default\":{\"properties\":{\"name\":{\"type\":\"keyword\",\"position\":1}}}}"));
    }

    @Test
    public void test_mappingMetadata_set_to_null() throws Throwable {
        IndexMetadata indexMetadata = IndexMetadata.builder(new RelationName("blob", "b1").indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", Version.V_4_7_0))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping((MappingMetadata) null) // here
            .build();

        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        IndexMetadata updatedMetadata = metadataIndexUpgrader.apply(indexMetadata, null);

        assertThat(updatedMetadata.mapping()).isNull();
    }

    private static CompressedXContent createDynamicStringMappingTemplate() throws IOException {
        // @formatter:off
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("strings")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("type", "keyword")
                                .field("doc_values", true)
                                .field("store", false)
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
            .endObject()
            .endObject();
        // @formatter:on

        return new CompressedXContent(BytesReference.bytes(builder));
    }

    @Test
    public void test_copy_to_migrated_to_sources() throws Throwable {
        IndexMetadata indexMetadata = IndexMetadata.builder(new RelationName("doc", "users").indexNameOrAlias())
            .settings(Settings.builder().put("index.version.created", Version.V_5_3_0))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(MappingConstants.FULLTEXT_MAPPING_5_3)
            .build();

        MetadataIndexUpgrader metadataIndexUpgrader = new MetadataIndexUpgrader();
        IndexMetadata updatedMetadata = metadataIndexUpgrader.apply(indexMetadata, null);

        MappingMetadata mapping = updatedMetadata.mapping();
        assertThat(mapping.source().string())
            .isEqualTo(MappingConstants.FULLTEXT_MAPPING_EXPECTED_IN_5_4);
    }

    @Test
    public void test_populateColumnPositionsImpl_method_with_empty_map() {
        Map<String, Object> map = new HashMap<>();
        MetadataIndexUpgrader.populateColumnPositionsImpl(map, map);
        assertThat(map).isEmpty();
        map.put("properties", new HashMap<>());
        MetadataIndexUpgrader.populateColumnPositionsImpl(map, Map.of());
        MetadataIndexUpgrader.populateColumnPositionsImpl(map, Map.of("properties", Map.of()));
        assertThat(map).isEqualTo(Map.of("properties", Map.of()));
    }

    @Test
    public void test_populateColumnPositionsImpl_method_without_missing_columns_still_overrides() {
        Map<String, Object> a = new HashMap<>();
        a.put("position", 1);
        Map<String, Object> indexMapping = Map.of("properties", Map.of("a", a));
        MetadataIndexUpgrader.populateColumnPositionsImpl(indexMapping,
            Map.of("properties", Map.of("a", Map.of("position", 10))));
        assertThat(a.get("position")).isEqualTo(10);
    }

    @Test
    public void test_populateColumnPositionsImpl_method_with_missing_columns_that_is_also_missing_from_template_mapping() {
        Map<String, Object> a = new HashMap<>();
        a.put("position", 1);
        Map<String, Object> indexMapping = Map.of("properties", Map.of("a", a));

        assertThatThrownBy(() -> MetadataIndexUpgrader.populateColumnPositionsImpl(
            indexMapping,
            Map.of("properties",
                Map.of("b", Map.of("position", 10)) // template-mapping is missing column 'a'
            )
        )).isExactlyInstanceOf(AssertionError.class)
            // template mappings must contain up-to-date and correct column positions that all relevant index mappings can reference.
            .hasMessage("the template mapping is missing column positions");
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

        MetadataIndexUpgrader.populateColumnPositionsImpl(
            map,
            Map.of("properties", Map.of(
                "a", Map.of("position", 1,
                    "properties", Map.of(
                        "b", Map.of("position", 2,
                            "properties", Map.of(
                                "d", Map.of("position", 3,
                                    "properties", Map.of()))))))));
        assertThat(map2.get("position")).isEqualTo(1);
        assertThat(map4.get("position")).isEqualTo(2);
        assertThat(map6.get("position")).isEqualTo(3);


        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        Map<String, Object> c = new HashMap<>();
        Map<String, Object> d = new HashMap<>();

        a.put("position", 1);
        b.put("position", 2);
        b.put("properties", Map.of("c", c, "d", d));
        c.put("position", 3);

        MetadataIndexUpgrader.populateColumnPositionsImpl(
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

        assertThat(a.get("position")).isEqualTo(1);
        assertThat(b.get("position")).isEqualTo(2);
        assertThat(c.get("position")).isEqualTo(3);
        assertThat(d.get("position")).isEqualTo(4);
    }

    @Test
    public void test_populateColumnPositionsImpl_method_overrides_duplicates() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();

        map.put("properties", Map.of("a", a, "b", b));
        a.put("position", 1);
        b.put("position", 1);

        MetadataIndexUpgrader.populateColumnPositionsImpl(
            map,
            Map.of("properties", Map.of(
                "a", Map.of("position", 3),
                "b", Map.of("position", 4))));

        assertThat(a.get("position")).isEqualTo(3);
        assertThat(b.get("position")).isEqualTo(4);
    }
}
