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

package io.crate.metadata.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.ColumnPositionResolver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.ContentPath;
import org.jetbrains.annotations.Nullable;

import io.crate.Constants;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.MapBuilder;
import io.crate.common.collections.Maps;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.server.xcontent.XContentHelper;

public class DDLClusterStateHelpers {

    public static IndexTemplateMetadata updateTemplate(IndexTemplateMetadata indexTemplateMetadata,
                                                       Map<String, Object> newMappings,
                                                       Map<String, Object> mappingsToRemove,
                                                       Settings newSettings,
                                                       IndexScopedSettings indexScopedSettings) {

        // merge mappings & remove mappings
        Map<String, Object> mapping = removeFromMapping(
            mergeTemplateMapping(indexTemplateMetadata, newMappings),
            mappingsToRemove);

        Settings settings = indexTemplateMetadata.settings();
        if (newSettings.isEmpty() == false) {
            // merge and validate settings (private settings must be filtered out, they are handled internally)
            Settings mergedSettings = Settings.builder()
                .put(indexTemplateMetadata.settings())
                .put(newSettings)
                .build()
                .filter(k -> indexScopedSettings.isPrivateSetting(k) == false);
            indexScopedSettings.validate(mergedSettings, true);
            settings = mergedSettings;
        }

        // wrap it in a type map if its not
        if (mapping.size() != 1 || mapping.containsKey(Constants.DEFAULT_MAPPING_TYPE) == false) {
            mapping = MapBuilder.<String, Object>newMapBuilder().put(Constants.DEFAULT_MAPPING_TYPE, mapping).map();
        }
        try {
            return new IndexTemplateMetadata.Builder(indexTemplateMetadata)
                .settings(settings)
                .putMapping(Strings.toString(JsonXContent.builder().map(mapping)))
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Set<IndexMetadata> indexMetadataSetFromIndexNames(Metadata metadata,
                                                             String[] indices,
                                                             IndexMetadata.State state) {
        Set<IndexMetadata> indicesMetadata = new HashSet<>();
        for (String indexName : indices) {
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata != null && indexMetadata.getState() != state) {
                indicesMetadata.add(indexMetadata);
            }
        }
        return indicesMetadata;
    }

    @Nullable
    static IndexTemplateMetadata templateMetadata(Metadata metadata, RelationName relationName) {
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        return metadata.templates().get(templateName);
    }

    public static Map<String, Object> removeFromMapping(Map<String, Object> mapping,
                                                        Map<String, Object> mappingsToRemove) {
        for (String key : mappingsToRemove.keySet()) {
            if (mapping.containsKey(key)) {
                if (mapping.get(key) instanceof Map) {
                    Object mapOrPropertyToRemove = mappingsToRemove.get(key);
                    if (mapOrPropertyToRemove == null) {
                        // The whole sub-map needs to be removed.
                        mapping.remove(key);
                    } else {
                        //noinspection unchecked
                        mapping.put(key, removeFromMapping((Map<String, Object>) mapping.get(key),
                            (Map<String, Object>) mappingsToRemove.get(key)));
                    }
                } else {
                    mapping.remove(key);
                }
            }
        }
        return mapping;
    }

    @VisibleForTesting
    static Map<String, Object> mergeTemplateMapping(IndexTemplateMetadata templateMetadata,
                                                    Map<String, Object> newMapping) {
        Map<String, Object> mergedMapping = new HashMap<>();
        Map<String, Object> mapping = parseMapping(templateMetadata.mapping().toString());
        Object o = mapping.get(Constants.DEFAULT_MAPPING_TYPE);
        assert o instanceof Map :
            "o must not be null and must be instance of Map";
        XContentHelper.update(mergedMapping, (Map) o, false);
        XContentHelper.update(mergedMapping, newMapping, false);
        populateColumnPositions(mergedMapping);
        return mergedMapping;
    }

    private static Map<String, Object> parseMapping(String mappingSource) {
        try (XContentParser parser = JsonXContent.JSON_XCONTENT
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping");
        }
    }

    public static boolean populateColumnPositions(Map<String, Object> mapping) {
        var columnPositionResolver = new ColumnPositionResolver<Map<String, Object>>();
        int[] maxColumnPosition = new int[]{0};
        populateColumnPositions(mapping, new ContentPath(), columnPositionResolver, maxColumnPosition);
        columnPositionResolver.updatePositions(maxColumnPosition[0]);
        return columnPositionResolver.numberOfColumnsToReposition() > 0;
    }

    private static void populateColumnPositions(Map<String, Object> mapping,
                                                ContentPath contentPath,
                                                ColumnPositionResolver<Map<String, Object>> columnPositionResolver,
                                                int[] maxColumnPosition) {

        Map<String, Object> properties = Maps.get(mapping, "properties");
        if (properties == null) {
            return;
        }
        for (var e : properties.entrySet()) {
            String name = e.getKey();
            contentPath.add(name);
            Map<String, Object> columnProperties = (Map<String, Object>) e.getValue();
            columnProperties = Maps.getOrDefault(columnProperties, "inner", columnProperties);
            assert columnProperties.containsKey("inner") || (columnProperties.containsKey("position") && columnProperties.get("position") != null)
                : "Column position is missing: " + name;
            // BWC compatibility with nodes < 5.1, position could be NULL if column is created on that nodes
            Integer position = (Integer) columnProperties.get("position");
            if (position != null) {
                if (position < 0) {
                    columnPositionResolver.addColumnToReposition(contentPath.pathAsText(""),
                        position,
                        columnProperties,
                        (cp, p) -> cp.put("position", p),
                        contentPath.currentDepth());
                } else {
                    maxColumnPosition[0] = Math.max(maxColumnPosition[0], position);
                }
            }
            populateColumnPositions(columnProperties, contentPath, columnPositionResolver, maxColumnPosition);
            contentPath.remove();
        }
    }
}
