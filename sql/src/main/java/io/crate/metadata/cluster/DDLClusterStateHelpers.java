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

package io.crate.metadata.cluster;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import io.crate.Constants;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

class DDLClusterStateHelpers {

    static IndexTemplateMetaData updateTemplate(IndexTemplateMetaData indexTemplateMetaData,
                                                Map<String, Object> newMappings,
                                                Map<String, Object> mappingsToRemove,
                                                Settings newSettings,
                                                BiConsumer<String, Settings> settingsValidator,
                                                Predicate<String> settingsFilter) {

        // merge mappings & remove mappings
        Map<String, Object> mapping = removeFromMapping(
            mergeTemplateMapping(indexTemplateMetaData, newMappings),
            mappingsToRemove);

        // merge settings
        final Settings settings = Settings.builder()
            .put(indexTemplateMetaData.settings())
            .put(newSettings)
            .normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX)
            // Private settings must not be (over-)written as they are generated, remove them.
            .build().filter(settingsFilter);

        settingsValidator.accept(indexTemplateMetaData.getName(), settings);

        // wrap it in a type map if its not
        if (mapping.size() != 1 || mapping.containsKey(Constants.DEFAULT_MAPPING_TYPE) == false) {
            mapping = MapBuilder.<String, Object>newMapBuilder().put(Constants.DEFAULT_MAPPING_TYPE, mapping).map();
        }
        try {
            return new IndexTemplateMetaData.Builder(indexTemplateMetaData)
                .settings(settings)
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, Strings.toString(XContentFactory.jsonBuilder().map(mapping)))
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Set<IndexMetaData> indexMetaDataSetFromIndexNames(MetaData metaData,
                                                             String[] indices,
                                                             IndexMetaData.State state) {
        Set<IndexMetaData> indicesMetaData = new HashSet<>();
        for (String indexName : indices) {
            IndexMetaData indexMetaData = metaData.index(indexName);
            if (indexMetaData != null && indexMetaData.getState() != state) {
                indicesMetaData.add(indexMetaData);
            }
        }
        return indicesMetaData;
    }

    @Nullable
    static IndexTemplateMetaData templateMetaData(MetaData metaData, RelationName relationName) {
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        return metaData.templates().get(templateName);
    }

    private static Map<String, Object> removeFromMapping(Map<String, Object> mapping,
                                                         Map<String, Object> mappingsToRemove) {
        for (String key : mappingsToRemove.keySet()) {
            if (mapping.containsKey(key)) {
                if (mapping.get(key) instanceof Map) {
                    //noinspection unchecked
                    mapping.put(key, removeFromMapping((Map<String, Object>) mapping.get(key),
                        (Map<String, Object>) mappingsToRemove.get(key)));
                } else {
                    mapping.remove(key);
                }
            }
        }
        return mapping;
    }

    @VisibleForTesting
    static Map<String, Object> mergeTemplateMapping(IndexTemplateMetaData templateMetaData,
                                                            Map<String, Object> newMapping) {
        Map<String, Object> mergedMapping = new HashMap<>();
        for (ObjectObjectCursor<String, CompressedXContent> cursor : templateMetaData.mappings()) {
            Map<String, Object> mapping = parseMapping(cursor.value.toString());
            Object o = mapping.get(Constants.DEFAULT_MAPPING_TYPE);
            assert o instanceof Map :
                "o must not be null and must be instance of Map";

            //noinspection unchecked
            XContentHelper.update(mergedMapping, (Map) o, false);
        }
        XContentHelper.update(mergedMapping, newMapping, false);
        return mergedMapping;
    }

    private static Map<String, Object> parseMapping(String mappingSource) {
        try (XContentParser parser = JsonXContent.jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping");
        }
    }
}
