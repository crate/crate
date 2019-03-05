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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class MetaDataIndexUpgrader implements UnaryOperator<IndexMetaData> {

    private final Logger logger;

    public MetaDataIndexUpgrader() {
        this.logger = LogManager.getLogger(MetaDataIndexUpgrader.class);
    }

    @Override
    public IndexMetaData apply(IndexMetaData indexMetaData) {
        return purgeDynamicStringTemplate(indexMetaData);
    }

    /**
     * Purges any dynamic template from the index metadata because they might be out-dated and the general default
     * template will apply any defaults for all indices.
     */
    private IndexMetaData purgeDynamicStringTemplate(IndexMetaData indexMetaData) {
        return IndexMetaData.builder(indexMetaData)
            .putMapping(purgeDynamicStringTemplate(
                indexMetaData.mapping(Constants.DEFAULT_MAPPING_TYPE),
                indexMetaData.getIndex().getName()))
            .build();
    }

    @VisibleForTesting
    MappingMetaData purgeDynamicStringTemplate(MappingMetaData mappingMetaData, String indexName) {
        Map<String, Object> oldMapping = mappingMetaData.getSourceAsMap();
        LinkedHashMap<String, Object> newMapping = new LinkedHashMap<>(oldMapping.size());
        for (Map.Entry<String, Object> entry : oldMapping.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();
            if (fieldName.equals("dynamic_templates")) {
                List<?> tmplNodes = (List<?>) fieldNode;
                List<Object> templates = new ArrayList<>();
                for (Object tmplNode : tmplNodes) {
                    //noinspection unchecked
                    Map<String, Object> tmpl = (Map<String, Object>) tmplNode;
                    if (tmpl.size() != 1) {
                        throw new MapperParsingException("A dynamic template must be defined with a name");
                    }
                    Map.Entry<String, Object> tmpEntry = tmpl.entrySet().iterator().next();
                    String templateName = tmpEntry.getKey();
                    if (templateName.equals("strings") == false) {
                        templates.add(tmplNode);
                    }
                }
                if (templates.size() > 0) {
                    newMapping.put(fieldName, templates);
                }
            } else {
                newMapping.put(fieldName, fieldNode);
            }
        }

        try {
            return new MappingMetaData(
                Constants.DEFAULT_MAPPING_TYPE,
                ImmutableMap.of(Constants.DEFAULT_MAPPING_TYPE, newMapping));
        } catch (IOException e) {
            logger.error("Failed to upgrade mapping for index '" + indexName + "'", e);
            return mappingMetaData;
        }
    }
}
