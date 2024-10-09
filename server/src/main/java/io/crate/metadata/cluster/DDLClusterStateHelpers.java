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

import java.util.Map;

import org.elasticsearch.cluster.metadata.ColumnPositionResolver;
import org.elasticsearch.index.mapper.ContentPath;

import io.crate.common.collections.Maps;

public class DDLClusterStateHelpers {

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
