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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Table;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class TableAnalyzer {

    public static Set<String> getIndexNames(List<Table> tables, Schemas schemas, ParameterContext parameterContext) {
        Set<String> indexNames = new HashSet<>(tables.size());
        for (Table nodeTable : tables) {
            TableInfo tableInfo = schemas.getTableInfo(
                TableIdent.of(nodeTable, parameterContext.defaultSchema()));
            Preconditions.checkArgument(tableInfo instanceof DocTableInfo,
                "operation cannot be performed on system and blob tables: table '%s'",
                tableInfo.ident().fqn());
            if (nodeTable.partitionProperties().isEmpty()) {
                indexNames.addAll(Arrays.asList(((DocTableInfo) tableInfo).concreteIndices()));
            } else {
                DocTableInfo docTableInfo = (DocTableInfo) tableInfo;
                PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                    docTableInfo,
                    nodeTable.partitionProperties(),
                    parameterContext.parameters()
                );
                if (!docTableInfo.partitions().contains(partitionName)) {
                    throw new PartitionUnknownException(tableInfo.ident().fqn(), partitionName.ident());
                }
                indexNames.add(partitionName.asIndexName());
            }
        }
        return indexNames;
    }
}
