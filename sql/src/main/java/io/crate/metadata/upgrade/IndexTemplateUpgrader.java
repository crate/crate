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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.metadata.DefaultTemplateService;
import io.crate.metadata.IndexParts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.crate.metadata.DefaultTemplateService.TEMPLATE_NAME;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.elasticsearch.common.settings.IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

public class IndexTemplateUpgrader implements UnaryOperator<Map<String, IndexTemplateMetaData>> {

    private final Logger logger;

    public IndexTemplateUpgrader() {
        this.logger = LogManager.getLogger(IndexTemplateUpgrader.class);
    }

    @Override
    public Map<String, IndexTemplateMetaData> apply(Map<String, IndexTemplateMetaData> templates) {
        HashMap<String, IndexTemplateMetaData> upgradedTemplates = archiveUnknownOrInvalidSettings(templates);
        try {
            upgradedTemplates.put(TEMPLATE_NAME, DefaultTemplateService.createDefaultIndexTemplateMetaData());
        } catch (IOException e) {
            logger.error("Error while trying to upgrade the default template", e);
        }
        return upgradedTemplates;
    }

    /**
     * Filter out all unknown/old/invalid settings. Archiving them *only* is not working as they would be "un-archived"
     * by {@link IndexTemplateMetaData.Builder#fromXContent} logic to prefix all settings with `index.` when applying
     * the new cluster state.
     */
    private HashMap<String, IndexTemplateMetaData> archiveUnknownOrInvalidSettings(Map<String, IndexTemplateMetaData> templates) {
        HashMap<String, IndexTemplateMetaData> upgradedTemplates = new HashMap<>(templates.size());
        for (Map.Entry<String, IndexTemplateMetaData> entry : templates.entrySet()) {
            IndexTemplateMetaData templateMetaData = entry.getValue();
            Settings.Builder settingsBuilder = Settings.builder().put(templateMetaData.settings());
            String templateName = entry.getKey();

            // only process partition table templates
            if (IndexParts.isPartitioned(templateName) == false) {
                upgradedTemplates.put(templateName, templateMetaData);
                continue;
            }

            Settings settings = DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
                settingsBuilder.build(), e -> { }, (e, ex) -> { })
                .filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false);

            IndexTemplateMetaData.Builder builder = IndexTemplateMetaData.builder(templateName)
                .patterns(templateMetaData.patterns())
                .order(templateMetaData.order())
                .settings(settings);
            try {
                for (ObjectObjectCursor<String, CompressedXContent> cursor : templateMetaData.getMappings()) {
                    builder.putMapping(cursor.key, cursor.value);
                }
            } catch (IOException e) {
                logger.error("Error while trying to upgrade template '" + templateName + "'", e);
                continue;
            }

            for (ObjectObjectCursor<String, AliasMetaData> container : templateMetaData.aliases()) {
                builder.putAlias(container.value);
            }
            upgradedTemplates.put(templateName, builder.build());
        }
        return upgradedTemplates;
    }
}
