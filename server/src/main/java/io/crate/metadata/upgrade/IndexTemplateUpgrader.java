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
import io.crate.common.collections.Maps;
import io.crate.metadata.IndexParts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ColumnPositionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

import static io.crate.metadata.doc.DocIndexMetadata.furtherColumnProperties;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.elasticsearch.common.settings.IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

public class IndexTemplateUpgrader implements UnaryOperator<Map<String, IndexTemplateMetadata>> {

    private final Logger logger;
    public static final String TEMPLATE_NAME = "crate_defaults";

    public IndexTemplateUpgrader() {
        this.logger = LogManager.getLogger(IndexTemplateUpgrader.class);
    }

    @Override
    public Map<String, IndexTemplateMetadata> apply(Map<String, IndexTemplateMetadata> templates) {
        HashMap<String, IndexTemplateMetadata> upgradedTemplates = archiveUnknownOrInvalidSettings(templates);
        upgradedTemplates.remove(TEMPLATE_NAME);
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
                    Map<String, Object> defaultMapping = Maps.get(mappingSource, "default");
                    boolean updated = populateColumnPositions(defaultMapping);
                    if (defaultMapping.containsKey("_all")) {
                        // Support for `_all` was removed (in favour of `copy_to`.
                        // We never utilized this but always set `_all: {enabled: false}` if you created a table using SQL in earlier version, so we can safely drop it.
                        defaultMapping.remove("_all");
                        updated = true;
                    }
                    if (updated) {
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

    public static boolean populateColumnPositions(Map<String, Object> mapping) {
        var columnPositionResolver = new ColumnPositionResolver<Map<String, Object>>();
        int[] maxColumnPosition = new int[]{0};
        populateColumnPositions("", mapping, 1, columnPositionResolver, new HashSet<>(), maxColumnPosition);
        columnPositionResolver.updatePositions(maxColumnPosition[0]);
        return columnPositionResolver.numberOfColumnsToReposition() > 0;
    }

    private static void populateColumnPositions(String parentName,
                                                Map<String, Object> mapping,
                                                int currentDepth,
                                                ColumnPositionResolver<Map<String, Object>> columnPositionResolver,
                                                Set<Integer> takenPositions,
                                                int[] maxColumnPosition) {

        Map<String, Object> properties = Maps.get(mapping, "properties");
        if (properties == null) {
            return;
        }
        Map<String, Map<String, Object>> childrenColumnProperties = new TreeMap<>(Comparator.naturalOrder());
        for (var e : properties.entrySet()) {
            String name = parentName + e.getKey();
            Map<String, Object> columnProperties = (Map<String, Object>) e.getValue();
            columnProperties = furtherColumnProperties(columnProperties);
            Integer position = (Integer) columnProperties.get("position");
            if (position == null || takenPositions.contains(position)) {
                columnPositionResolver.addColumnToReposition(name,
                                                             null,
                                                             columnProperties,
                                                             (cp, p) -> cp.put("position", p),
                                                             currentDepth);
            } else {
                takenPositions.add(position);
                maxColumnPosition[0] = Math.max(maxColumnPosition[0], position);
            }
            childrenColumnProperties.put(name, columnProperties);
        }
        // Breadth-First traversal
        for (var childColumnProperties : childrenColumnProperties.entrySet()) {
            populateColumnPositions(childColumnProperties.getKey(),
                                    childColumnProperties.getValue(),
                                    currentDepth + 1,
                                    columnPositionResolver,
                                    takenPositions,
                                    maxColumnPosition);
        }
    }
}
