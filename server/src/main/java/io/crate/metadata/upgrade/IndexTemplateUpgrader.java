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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.metadata.DefaultTemplateService;
import io.crate.metadata.IndexParts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.crate.metadata.DefaultTemplateService.TEMPLATE_NAME;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.elasticsearch.common.settings.IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

public class IndexTemplateUpgrader implements UnaryOperator<Map<String, IndexTemplateMetadata>> {

    private final Logger logger;

    public IndexTemplateUpgrader() {
        this.logger = LogManager.getLogger(IndexTemplateUpgrader.class);
    }

    @Override
    public Map<String, IndexTemplateMetadata> apply(Map<String, IndexTemplateMetadata> templates) {
        HashMap<String, IndexTemplateMetadata> upgradedTemplates = archiveUnknownOrInvalidSettings(templates);
        try {
            upgradedTemplates.put(TEMPLATE_NAME, DefaultTemplateService.createDefaultIndexTemplateMetadata());
        } catch (IOException e) {
            logger.error("Error while trying to upgrade the default template", e);
        }
        return upgradedTemplates;
    }

    /**
     * Filter out all unknown/old/invalid settings. Archiving them *only* is not working as they would be "un-archived"
     * by {@link IndexTemplateMetadata.Builder#fromXContent} logic to prefix all settings with `index.` when applying
     * the new cluster state.
     */
    private HashMap<String, IndexTemplateMetadata> archiveUnknownOrInvalidSettings(Map<String, IndexTemplateMetadata> templates) {
        HashMap<String, IndexTemplateMetadata> upgradedTemplates = new HashMap<>(templates.size());
        for (Map.Entry<String, IndexTemplateMetadata> entry : templates.entrySet()) {
            IndexTemplateMetadata templateMetadata = entry.getValue();
            Settings.Builder settingsBuilder = Settings.builder().put(templateMetadata.settings());
            String templateName = entry.getKey();

            // only process partition table templates
            if (IndexParts.isPartitioned(templateName) == false) {
                upgradedTemplates.put(templateName, templateMetadata);
                continue;
            }

            Settings settings = DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
                settingsBuilder.build(), e -> { }, (e, ex) -> { })
                .filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false);

            IndexTemplateMetadata.Builder builder = IndexTemplateMetadata.builder(templateName)
                .patterns(templateMetadata.patterns())
                .order(templateMetadata.order())
                .settings(settings);
            try {
                for (ObjectObjectCursor<String, CompressedXContent> cursor : templateMetadata.getMappings()) {
                    var mappingSource = XContentHelper.toMap(cursor.value.compressedReference(), XContentType.JSON);

                    Object defaultMapping = mappingSource.get("default");
                    if (defaultMapping instanceof Map && ((Map<?, ?>) defaultMapping).containsKey("_all")) {
                        Map<?, ?> mapping = (Map<?, ?>) defaultMapping;

                        // Support for `_all` was removed (in favour of `copy_to`.
                        // We never utilized this but always set `_all: {enabled: false}` if you created a table using SQL in earlier version, so we can safely drop it.
                        mapping.remove("_all");
                        builder.putMapping(
                            cursor.key,
                            new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().value(mappingSource))));
                    } else {
                        builder.putMapping(cursor.key, cursor.value);
                    }
                }
            } catch (IOException e) {
                logger.error("Error while trying to upgrade template '" + templateName + "'", e);
                continue;
            }

            for (ObjectObjectCursor<String, AliasMetadata> container : templateMetadata.aliases()) {
                builder.putAlias(container.value);
            }
            upgradedTemplates.put(templateName, builder.build());
        }
        return upgradedTemplates;
    }
}
