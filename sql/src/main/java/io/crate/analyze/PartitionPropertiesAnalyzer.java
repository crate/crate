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
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PartitionPropertiesAnalyzer {

    public static String toPartitionIdent(TableInfo tableInfo,
                                          GenericProperties partitionProperties,
                                          Object[] parameters) {
        Map<String, List<Expression>> properties = partitionProperties.properties();
        Preconditions.checkArgument(properties.size() == tableInfo.partitionedBy().size(),
                "The table \"%s\" is partitioned by %s columns but the PARTITION clause contains %s columns",
                tableInfo.ident().name(),
                tableInfo.partitionedBy().size(),
                properties.size()
        );

        String[] values = new String[properties.size()];
        for (Map.Entry<String, List<Expression>> stringListEntry : properties.entrySet()) {
            Object value = ExpressionToObjectVisitor.convert(
                    stringListEntry.getValue().get(0), parameters);

            int idx = tableInfo.partitionedBy().indexOf(stringListEntry.getKey());
            try {
                ReferenceInfo referenceInfo = tableInfo.partitionedByColumns().get(idx);
                values[idx] = referenceInfo.type().value(value).toString();
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException(
                        String.format("\"%s\" is no known partition column", stringListEntry.getKey()));
            }
        }
        return PartitionName.encodeIdent(Arrays.asList(values));
    }
}
