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

import io.crate.Constants;
import io.crate.executor.transport.AlterTableOperation;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class DDLClusterStateHelpers {

    static IndexTemplateMetaData updateTemplate(IndexTemplateMetaData indexTemplateMetaData,
                                                Map<String, Object> newMappings,
                                                Map<String, Object> mappingsToRemove,
                                                Settings newSettings) {

        // merge mappings
        Map<String, Object> mapping = AlterTableOperation.mergeTemplateMapping(indexTemplateMetaData, newMappings);

        // remove mappings
        mapping = AlterTableOperation.removeFromMapping(mapping, mappingsToRemove);

        // merge settings
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(indexTemplateMetaData.settings());
        settingsBuilder.put(newSettings);

        // wrap it in a type map if its not
        if (mapping.size() != 1 || mapping.containsKey(Constants.DEFAULT_MAPPING_TYPE) == false) {
            mapping = MapBuilder.<String, Object>newMapBuilder().put(Constants.DEFAULT_MAPPING_TYPE, mapping).map();
        }
        try {
            return new IndexTemplateMetaData.Builder(indexTemplateMetaData)
                .settings(settingsBuilder)
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, XContentFactory.jsonBuilder().map(mapping).string())
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
    static IndexTemplateMetaData templateMetaData(MetaData metaData, TableIdent tableIdent) {
        String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());
        return metaData.templates().get(templateName);
    }
}
