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

package io.crate.cluster.commands;

import static io.crate.cluster.commands.FixCorruptedMetadataCommand.fixInconsistencyBetweenIndexAndTemplates;
import static io.crate.cluster.commands.FixCorruptedMetadataCommand.fixIndexName;
import static io.crate.cluster.commands.FixCorruptedMetadataCommand.fixNameOfTemplateMetadata;
import static io.crate.cluster.commands.FixCorruptedMetadataCommand.fixTemplateName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;

public class FixCorruptedMetadataCommandTest {

    @Test
    public void test_method_fixNameOfTemplateMetadata_fixes_corrupted_name_only() throws IOException {
        String corruptedName = ".partitioned.m1.s1.";
        String mapping = "{\"default\":{\"_meta\":{\"partitioned_by\":[[\"t\",\"boolean\"]]},\"dynamic\":\"strict\",\"properties\":{\"t\":{\"index\":false,\"position\":1,\"type\":\"boolean\"}}}}";
        IndexTemplateMetadata.Builder corruptedBuilder = new IndexTemplateMetadata.Builder(corruptedName);
        corruptedBuilder.patterns(List.of(corruptedName + "*"));
        corruptedBuilder.settings(Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(300)));
        corruptedBuilder.putMapping(mapping);
        IndexTemplateMetadata corrupted = corruptedBuilder.build();

        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(corruptedName, corrupted);
        Metadata.Builder fixedMetadata = new Metadata.Builder();
        fixNameOfTemplateMetadata(mapBuilder.build(), fixedMetadata);

        // only the name of the corrupted template is fixed
        String fixedName = "m1..partitioned.s1.";
        IndexTemplateMetadata fixed = fixedMetadata.getTemplate(fixedName);
        assertThat(fixed).isNotNull();
        assertThat(fixed.patterns()).hasSize(1);
        assertThat(fixed.patterns().get(0)).isEqualTo(fixedName + "*");
        assertThat(fixed.mapping().toString()).hasToString(mapping);
        assertThat(fixed.settings().get(INDEX_REFRESH_INTERVAL_SETTING.getKey())).isEqualTo("300ms");
    }

    @Test
    public void test_method_fixNameOfTemplateMetadata_fixes_corrupted_name_and_overwrites_existing_template() throws IOException {
        String corruptedName = ".partitioned.m1.s1.";
        String mapping = "{\"default\":{\"_meta\":{\"partitioned_by\":[[\"t\",\"boolean\"]]},\"dynamic\":\"strict\",\"properties\":{\"t\":{\"index\":false,\"position\":1,\"type\":\"boolean\"}}}}";
        IndexTemplateMetadata.Builder corruptedBuilder = new IndexTemplateMetadata.Builder(corruptedName);
        corruptedBuilder.patterns(List.of(corruptedName + "*"));
        corruptedBuilder.settings(Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(300)));
        corruptedBuilder.putMapping(mapping);
        IndexTemplateMetadata corrupted = corruptedBuilder.build();

        // to be overwritten
        String nameOfExistingTemplate = "m1..partitioned.s1.";
        String mappingOfExistingTemplate = "{\"default\":{\"_meta\":{\"partitioned_by\":[[\"s\",\"keyword\"]]},\"dynamic\":\"strict\",\"properties\":{\"s\":{\"length_limit\":1,\"index\":false,\"position\":1,\"blank_padding\":true,\"type\":\"keyword\"}}}}";
        IndexTemplateMetadata.Builder existingBuilder = new IndexTemplateMetadata.Builder(nameOfExistingTemplate);
        existingBuilder.patterns(List.of(nameOfExistingTemplate + "*"));
        existingBuilder.settings(Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(400)));
        existingBuilder.putMapping(mappingOfExistingTemplate);
        IndexTemplateMetadata existingTemplate = existingBuilder.build();

        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(corruptedName, corrupted);
        mapBuilder.put(nameOfExistingTemplate, existingTemplate);
        Metadata.Builder fixedMetadata = new Metadata.Builder();
        fixNameOfTemplateMetadata(mapBuilder.build(), fixedMetadata);

        // only the name of the corrupted template is fixed
        String fixedName = "m1..partitioned.s1.";
        IndexTemplateMetadata fixed = fixedMetadata.getTemplate(fixedName);
        assertThat(fixed).isNotNull();
        assertThat(fixed.patterns()).hasSize(1);
        assertThat(fixed.patterns().get(0)).isEqualTo(fixedName + "*");
        assertThat(fixed.mapping()).hasToString(mapping);
        assertThat(fixed.settings().get(INDEX_REFRESH_INTERVAL_SETTING.getKey())).isEqualTo("300ms");
    }

    @Test
    public void test_method_fixNameOfTemplateMetadata_valid_template_is_untouched() throws IOException {
        String corruptedName = ".partitioned.m1.s1.";
        String mapping = "{\"default\":{\"_meta\":{\"partitioned_by\":[[\"t\",\"boolean\"]]},\"dynamic\":\"strict\",\"properties\":{\"t\":{\"index\":false,\"position\":1,\"type\":\"boolean\"}}}}";
        IndexTemplateMetadata.Builder corruptedBuilder = new IndexTemplateMetadata.Builder(corruptedName);
        corruptedBuilder.patterns(List.of(corruptedName + "*"));
        corruptedBuilder.settings(Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(300)));
        corruptedBuilder.putMapping(mapping);
        IndexTemplateMetadata corrupted = corruptedBuilder.build();

        String nameOfExistingTemplate = "m1..partitioned.s1.";
        String mappingOfExistingTemplate = "{\"default\":{\"_meta\":{\"partitioned_by\":[[\"s\",\"keyword\"]]},\"dynamic\":\"strict\",\"properties\":{\"s\":{\"length_limit\":1,\"index\":false,\"position\":1,\"blank_padding\":true,\"type\":\"keyword\"}}}}";
        IndexTemplateMetadata.Builder existingBuilder = new IndexTemplateMetadata.Builder(nameOfExistingTemplate);
        existingBuilder.patterns(List.of(nameOfExistingTemplate + "*"));
        existingBuilder.settings(Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(400)));
        existingBuilder.putMapping(mappingOfExistingTemplate);
        IndexTemplateMetadata existingTemplate = existingBuilder.build();

        String dummyName = "m2..partitioned.dummy.";
        String dummyMapping = "{\"default\":{\"_meta\":{\"partitioned_by\":[[\"dummy\",\"boolean\"]]},\"dynamic\":\"strict\",\"properties\":{\"dummy\":{\"index\":false,\"position\":1,\"type\":\"boolean\"}}}}";
        IndexTemplateMetadata.Builder dummyBuilder = new IndexTemplateMetadata.Builder(dummyName);
        dummyBuilder.patterns(List.of(dummyName + "*"));
        dummyBuilder.settings(Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(500)));
        dummyBuilder.putMapping(dummyMapping);
        IndexTemplateMetadata dummy = dummyBuilder.build();

        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(corruptedName, corrupted);
        mapBuilder.put(dummyName, dummy);
        mapBuilder.put(nameOfExistingTemplate, existingTemplate);
        Metadata.Builder fixedMetadata = new Metadata.Builder();
        fixNameOfTemplateMetadata(mapBuilder.build(), fixedMetadata);

        // only the name of the corrupted template is fixed
        String fixedName = "m1..partitioned.s1.";
        IndexTemplateMetadata fixed = fixedMetadata.getTemplate(fixedName);
        assertThat(fixed).isNotNull();
        assertThat(fixed.patterns()).hasSize(1);
        assertThat(fixed.patterns().get(0)).isEqualTo(fixedName + "*");
        assertThat(fixed.mapping()).hasToString(mapping);
        assertThat(fixed.settings().get(INDEX_REFRESH_INTERVAL_SETTING.getKey())).isEqualTo("300ms");

        //dummy is untouched
        IndexTemplateMetadata dummyTemplate = fixedMetadata.getTemplate(dummyName);
        assertThat(dummyTemplate).isNotNull();
        assertThat(dummyTemplate.patterns()).hasSize(1);
        assertThat(dummyTemplate.patterns().get(0)).isEqualTo(dummyName + "*");
        assertThat(dummyTemplate.mapping()).hasToString(dummyMapping);
        assertThat(dummyTemplate.settings().get(INDEX_REFRESH_INTERVAL_SETTING.getKey())).isEqualTo("500ms");
    }

    @Test
    public void test_method_fixTemplateName() {
        assertThat(fixTemplateName(".partitioned.partitioned.")).isNull();
        assertThat(fixTemplateName("m..partitioned.t.")).isNull();
        assertThat(fixTemplateName(".partitioned.m.t.")).isEqualTo(new RelationName("m", "t"));
    }

    @Test
    public void test_method_fixIndexName() {
        // valid names
        assertThat(fixIndexName("table")).isNull();
        assertThat(fixIndexName("myschema.table")).isNull();
        assertThat(fixIndexName("myschema.partitioned.table")).isNull();
        assertThat(fixIndexName(".partitioned.partitioned.04132")).isNull(); // partitioned table named partitioned in doc schema

        // invalid name that is to be fixed
        assertThat(fixIndexName(".partitioned.myschema.table.0123"))
            .isEqualTo("myschema..partitioned.table.0123");
    }


    @Test
    public void test_fixInconsistencyBetweenIndexAndTemplates_with_invalid_non_partitioned_indexMetadata_containing_partitioned_by_column() throws IOException {
        IndexMetadata.Builder corruptedBuilder = IndexMetadata.builder("m7.s7");
        var corruptedSettings = Settings.builder();

        // contains partitioned_by columns -- corrupted
        String mappingForCorrupted =
            """
                {"default":{"dynamic":"strict","_meta":{"partitioned_by":[["a","integer"],["b","integer"]]},
                "properties":{"a":{"type":"integer","index":false,"position":1},"b":{"type":"integer","index":false,"position":2}}}}
                """.strip();
        corruptedSettings.put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.version.created", org.elasticsearch.Version.CURRENT);
        corruptedBuilder.putMapping(mappingForCorrupted);
        corruptedBuilder.settings(corruptedSettings);

        IndexMetadata corruptedMetadata = corruptedBuilder.build();

        Metadata.Builder upgradedMetadata = Metadata.builder();
        upgradedMetadata.put(IndexMetadata.builder(corruptedMetadata));

        fixInconsistencyBetweenIndexAndTemplates(corruptedMetadata, upgradedMetadata);

        // since the indexMetadata is invalid, it is now partitioned
        assertThat(upgradedMetadata.get("m7.s7")).isNull();
        IndexMetadata fixedIndexMetadata = upgradedMetadata.get("m7..partitioned.s7.08000");
        assertThat(fixedIndexMetadata.mapping().source()).isEqualTo(corruptedMetadata.mapping().source());
        assertThat(fixedIndexMetadata.getSettings().getAsStructuredMap())
            .isEqualTo(corruptedMetadata.getSettings().getAsStructuredMap());

        // Also indexTemplateMetadata is created accordingly.
        IndexTemplateMetadata convertedFromIndexMetadata = upgradedMetadata.getTemplate("m7..partitioned.s7.");
        assertThat(convertedFromIndexMetadata).isNotNull();
        assertThat(convertedFromIndexMetadata.settings().getAsStructuredMap())
            .isEqualTo(corruptedMetadata.getSettings().getAsStructuredMap());
        assertThat(convertedFromIndexMetadata.mapping()).isEqualTo(corruptedMetadata.mapping().source());
    }

    @Test
    public void test_fixInconsistencyBetweenIndexAndTemplates_with_invalid_non_partitioned_indexMetadata_containing_partitioned_by_column_and_indexTemplateMetadata() throws IOException {
        IndexMetadata.Builder corruptedBuilder = IndexMetadata.builder("m7.s7");
        var corruptedSettings = Settings.builder();

        // contains partitioned_by columns -- corrupted
        String mappingForCorrupted =
            """
                {"default":{"dynamic":"strict","_meta":{"partitioned_by":[["a","integer"],["b","integer"]]},
                "properties":{"a":{"type":"integer","index":false,"position":1},"b":{"type":"integer","index":false,"position":2}}}}
                """.strip();
        corruptedSettings.put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.version.created", org.elasticsearch.Version.CURRENT);
        corruptedBuilder.putMapping(mappingForCorrupted);
        corruptedBuilder.settings(corruptedSettings);

        IndexMetadata corruptedMetadata = corruptedBuilder.build();

        Metadata.Builder upgradedMetadata = Metadata.builder();
        upgradedMetadata.put(IndexMetadata.builder(corruptedMetadata));

        // an existing template that will conflict with the template to be created.
        String existingTemplateName = "m7..partitioned.s7.";
        IndexTemplateMetadata.Builder templateBuilder = new IndexTemplateMetadata.Builder(existingTemplateName);
        templateBuilder.patterns(List.of(existingTemplateName + "*")).putMapping("{\"default\": {}}");
        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(existingTemplateName, templateBuilder.build());
        upgradedMetadata.templates(mapBuilder.build());

        fixInconsistencyBetweenIndexAndTemplates(corruptedMetadata, upgradedMetadata);

        // since the indexMetadata is invalid, it is now partitioned
        assertThat(upgradedMetadata.get("m7.s7")).isNull();
        IndexMetadata fixedIndexMetadata = upgradedMetadata.get("m7..partitioned.s7.08000");
        assertThat(fixedIndexMetadata.mapping().source()).isEqualTo(corruptedMetadata.mapping().source());
        assertThat(fixedIndexMetadata.getSettings().getAsStructuredMap())
            .isEqualTo(corruptedMetadata.getSettings().getAsStructuredMap());

        // Also indexTemplateMetadata is created accordingly -- this help verifies that the existing template is overwritten
        IndexTemplateMetadata convertedFromIndexMetadata = upgradedMetadata.getTemplate(existingTemplateName);
        assertThat(convertedFromIndexMetadata).isNotNull();
        assertThat(convertedFromIndexMetadata.settings().getAsStructuredMap())
            .isEqualTo(corruptedMetadata.getSettings().getAsStructuredMap());
        assertThat(convertedFromIndexMetadata.mapping()).isEqualTo(corruptedMetadata.mapping().source());
    }

    @Test
    public void test_fixInconsistencyBetweenIndexAndTemplates_with_invalid_indexTemplateMetadata() throws IOException {
        // if a table is not partitioned, there should not be any existing indexTemplateMetadata

        IndexMetadata.Builder nonPartitionedBuilder = IndexMetadata.builder("m7.s7");
        var nonPartitionedSettings = Settings.builder();

        String mappingForNonPartitioned = """
            {"default":{"dynamic":"strict","_meta":{},"properties":{"a":{"type":"integer","position":1}}}}
            """.strip();
        nonPartitionedSettings.put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.version.created", org.elasticsearch.Version.CURRENT);
        nonPartitionedBuilder.putMapping(mappingForNonPartitioned);
        nonPartitionedBuilder.settings(nonPartitionedSettings);

        IndexMetadata nonPartitioned = nonPartitionedBuilder.build();

        Metadata.Builder fixedMetadata = Metadata.builder();
        fixedMetadata.put(IndexMetadata.builder(nonPartitioned));

        // to be removed
        String invalidTemplateName = "m7..partitioned.s7.";
        IndexTemplateMetadata.Builder templateBuilder = new IndexTemplateMetadata.Builder(invalidTemplateName);
        templateBuilder.patterns(List.of(invalidTemplateName + "*")).putMapping("{\"default\": {}}");
        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(invalidTemplateName, templateBuilder.build());
        fixedMetadata.templates(mapBuilder.build());

        fixInconsistencyBetweenIndexAndTemplates(nonPartitioned, fixedMetadata);
        var afterFix = fixedMetadata.get("m7.s7");
        assertThat(afterFix).isNotNull();
        assertThat(afterFix.mapping().source()).hasToString(mappingForNonPartitioned);
        assertThat(afterFix.getSettings().getAsStructuredMap())
            .hasToString("{index={name=m7.s7, number_of_shards=1, number_of_replicas=1, version={created=" +
                         Version.CURRENT.internalId + "}}}");

        // indexMetadata named 'm7.s7' and indexTemplateMetadata 'm7..partitioned.s7.' cannot co-exist.
        IndexTemplateMetadata existingTemplate = fixedMetadata.getTemplate(invalidTemplateName);
        assertThat(existingTemplate).isNull();
    }
}
