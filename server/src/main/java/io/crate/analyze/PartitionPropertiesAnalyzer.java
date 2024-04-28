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

package io.crate.analyze;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.cluster.metadata.Metadata;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.types.DataTypes;

public class PartitionPropertiesAnalyzer {

    private static Map<ColumnIdent, Object> assignmentsToMap(List<Assignment<Object>> assignments) {
        HashMap<ColumnIdent, Object> map = new HashMap<>(assignments.size());
        for (Assignment<Object> assignment : assignments) {
            map.put(
                ColumnIdent.fromPath(assignment.columnName().toString()),
                assignment.expression()
            );
        }
        return map;
    }

    public static PartitionName toPartitionName(RelationName relationName, List<Assignment<Object>> partitionProperties) {

        String[] values = new String[partitionProperties.size()];
        int idx = 0;
        for (Assignment<Object> o : partitionProperties) {
            values[idx++] = DataTypes.STRING.implicitCast(o.expression());
        }
        return new PartitionName(relationName, List.of(values));
    }

    public static PartitionName toPartitionName(DocTableInfo tableInfo,
                                                List<Assignment<Object>> partitionProperties) {
        if (!tableInfo.isPartitioned()) {
            throw new IllegalArgumentException("table '" + tableInfo.ident().fqn() + "' is not partitioned");
        }
        if (partitionProperties.size() != tableInfo.partitionedBy().size()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "The table \"%s\" is partitioned by %s columns but the PARTITION clause contains %s columns",
                tableInfo.ident().fqn(),
                tableInfo.partitionedBy().size(),
                partitionProperties.size()
            ));
        }
        Map<ColumnIdent, Object> properties = assignmentsToMap(partitionProperties);
        String[] values = new String[properties.size()];

        for (Map.Entry<ColumnIdent, Object> entry : properties.entrySet()) {
            Object value = entry.getValue();

            int idx = tableInfo.partitionedBy().indexOf(entry.getKey());
            try {
                Reference reference = tableInfo.partitionedByColumns().get(idx);
                Object converted = reference.valueType().implicitCast(value);
                values[idx] = DataTypes.STRING.implicitCast(converted);
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "\"%s\" is no known partition column", entry.getKey().sqlFqn()));
            }
        }
        return new PartitionName(tableInfo.ident(), Arrays.asList(values));
    }

    @Nullable
    public static PartitionName createPartitionName(List<Assignment<Object>> partitionsProperties,
                                                    DocTableInfo tableInfo,
                                                    Metadata metadata) {
        if (partitionsProperties.isEmpty()) {
            return null;
        }
        PartitionName partitionName = toPartitionName(
            tableInfo,
            partitionsProperties
        );
        if (tableInfo.getPartitions(metadata).contains(partitionName) == false) {
            throw new IllegalArgumentException("Referenced partition \"" + partitionName + "\" does not exist.");
        }
        return partitionName;
    }
}
