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

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ColumnPositionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;

import io.crate.common.collections.Maps;

public class IndexTemplateUpgrader implements UnaryOperator<Map<String, IndexTemplateMetadata>> {

    private final Logger logger;
    public static final String TEMPLATE_NAME = "crate_defaults";

    public IndexTemplateUpgrader() {
        this.logger = LogManager.getLogger(IndexTemplateUpgrader.class);
    }

    @Override
    public Map<String, IndexTemplateMetadata> apply(Map<String, IndexTemplateMetadata> templates) {
        return templates;
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
            columnProperties = Maps.getOrDefault(columnProperties, "inner", columnProperties);
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
