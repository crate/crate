/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Preconditions;
import io.crate.PartitionName;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionPropertiesAnalyzer {

    public static Map<ColumnIdent, Object> assignmentsToMap(List<Assignment> assignments,
                                                       Object[] parameters) {
        Map<ColumnIdent, Object> map = new HashMap<>(assignments.size());
        for (Assignment assignment : assignments) {
            map.put(
                    ColumnIdent.fromPath(ExpressionToStringVisitor.convert(assignment.columnName(), parameters)),
                    ExpressionToObjectVisitor.convert(assignment.expression(), parameters)
                    );
        }
        return map;
    }

    public static PartitionName toPartitionName(TableInfo tableInfo,
                                                List<Assignment> partitionProperties,
                                                Object[] parameters) {
        Preconditions.checkArgument(tableInfo.isPartitioned(), "table '%s' is not partitioned", tableInfo.ident().name());
        Preconditions.checkArgument(partitionProperties.size() == tableInfo.partitionedBy().size(),
                "The table \"%s\" is partitioned by %s columns but the PARTITION clause contains %s columns",
                tableInfo.ident().name(),
                tableInfo.partitionedBy().size(),
                partitionProperties.size()
        );
        Map<ColumnIdent, Object> properties = assignmentsToMap(partitionProperties, parameters);
        BytesRef[] values = new BytesRef[properties.size()];

        for (Map.Entry<ColumnIdent, Object> entry : properties.entrySet()) {
            Object value = entry.getValue();

            int idx = tableInfo.partitionedBy().indexOf(entry.getKey());
            try {
                ReferenceInfo referenceInfo = tableInfo.partitionedByColumns().get(idx);
                Object converted = referenceInfo.type().value(value);
                values[idx] = converted == null ? null : DataTypes.STRING.value(converted);
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException(
                        String.format("\"%s\" is no known partition column", entry.getKey().fqn()));
            }
        }
        return new PartitionName(tableInfo.ident().name(), Arrays.asList(values));
    }

    public static String toPartitionIdent(TableInfo tableInfo,
                                          List<Assignment> partitionProperties,
                                          Object[] parameters) {
        return toPartitionName(tableInfo, partitionProperties, parameters).ident();
    }
}
