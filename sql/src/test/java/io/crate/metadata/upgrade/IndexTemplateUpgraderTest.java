/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.upgrade;

import io.crate.metadata.DefaultTemplateService;
import io.crate.metadata.PartitionName;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.crate.metadata.DefaultTemplateService.TEMPLATE_NAME;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class IndexTemplateUpgraderTest {

    @Test
    public void testDefaultTemplateIsUpgraded() throws IOException {
        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();

        HashMap<String, IndexTemplateMetaData> templates = new HashMap<>();
        IndexTemplateMetaData oldTemplate = IndexTemplateMetaData.builder(TEMPLATE_NAME)
            .patterns(Collections.singletonList("*"))
            .build();
        templates.put(TEMPLATE_NAME, oldTemplate);

        Map<String, IndexTemplateMetaData> upgradedTemplates = upgrader.apply(templates);
        assertThat(upgradedTemplates.get(TEMPLATE_NAME), is(DefaultTemplateService.createDefaultIndexTemplateMetaData()));
    }

    @Test
    public void testArchivedSettingsAreRemovedOnPartitionedTableTemplates() {
        IndexTemplateUpgrader upgrader = new IndexTemplateUpgrader();

        Settings settings = Settings.builder()
            .put(ARCHIVED_SETTINGS_PREFIX + "some.setting", true)   // archived, must be filtered out
            .put(SETTING_NUMBER_OF_SHARDS, 4)
            .build();

        HashMap<String, IndexTemplateMetaData> templates = new HashMap<>();
        String partitionTemplateName = PartitionName.templateName("doc", "t1");
        IndexTemplateMetaData oldPartitionTemplate = IndexTemplateMetaData.builder(partitionTemplateName)
            .settings(settings)
            .patterns(Collections.singletonList("*"))
            .build();
        templates.put(partitionTemplateName, oldPartitionTemplate);

        String nonPartitionTemplateName = "non-partition-template";
        IndexTemplateMetaData oldNonPartitionTemplate = IndexTemplateMetaData.builder(nonPartitionTemplateName)
            .settings(settings)
            .patterns(Collections.singletonList("*"))
            .build();
        templates.put(nonPartitionTemplateName, oldNonPartitionTemplate);

        Map<String, IndexTemplateMetaData> upgradedTemplates = upgrader.apply(templates);
        IndexTemplateMetaData upgradedTemplate = upgradedTemplates.get(partitionTemplateName);
        assertThat(upgradedTemplate.settings().keySet(), contains(SETTING_NUMBER_OF_SHARDS));

        // ensure all other attributes remains the same
        assertThat(upgradedTemplate.mappings(), is(oldPartitionTemplate.mappings()));
        assertThat(upgradedTemplate.patterns(), is(oldPartitionTemplate.patterns()));
        assertThat(upgradedTemplate.order(), is(oldPartitionTemplate.order()));
        assertThat(upgradedTemplate.aliases(), is(oldPartitionTemplate.aliases()));

        // ensure non partitioned table templates are untouched
        assertThat(upgradedTemplates.get(nonPartitionTemplateName), is(oldNonPartitionTemplate));
    }

    @Test
    public void testInvalidSettingIsRemovedForTemplateInCustomSchema() {
        Settings settings = Settings.builder().put("index.recovery.initial_shards", "quorum").build();
        String templateName = PartitionName.templateName("foobar", "t1");
        IndexTemplateMetaData template = IndexTemplateMetaData.builder(templateName)
            .settings(settings)
            .patterns(Collections.singletonList("*"))
            .build();

        IndexTemplateUpgrader indexTemplateUpgrader = new IndexTemplateUpgrader();
        Map<String, IndexTemplateMetaData> result = indexTemplateUpgrader.apply(Collections.singletonMap(templateName, template));

        assertThat(
            "Outdated setting `index.recovery.initial_shards` must be removed",
            result.get(templateName).settings().hasValue("index.recovery.initial_shards"),
            is(false)
        );
    }
}
