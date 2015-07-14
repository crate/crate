/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Preconditions;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Singleton
public class RefreshTableAnalyzer extends DefaultTraversalVisitor<RefreshTableAnalyzedStatement, Analysis> {

    private final Schemas schemas;

    @Inject
    public RefreshTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public RefreshTableAnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return super.process(node, analysis);
    }

    @Override
    public RefreshTableAnalyzedStatement visitRefreshStatement(RefreshStatement node, Analysis analysis) {
        Set<String> indexNames = new HashSet<>(node.tables().size());
        for (Table nodeTable : node.tables()) {
            TableInfo tableInfo = schemas.getTableInfo(
                    TableIdent.of(nodeTable, analysis.parameterContext().defaultSchema()));
            Preconditions.checkArgument(tableInfo instanceof DocTableInfo,
                    "table '%s' cannot be refreshed",
                    tableInfo.ident().fqn());
            if (nodeTable.partitionProperties().isEmpty()) {
                indexNames.addAll(Arrays.asList(((DocTableInfo) tableInfo).concreteIndices()));
            } else {
                DocTableInfo docTableInfo = (DocTableInfo) tableInfo;
                PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                        docTableInfo,
                        nodeTable.partitionProperties(),
                        analysis.parameterContext().parameters()
                );
                if (!docTableInfo.partitions().contains(partitionName)) {
                    throw new PartitionUnknownException(tableInfo.ident().fqn(), partitionName.ident());
                }
                indexNames.add(partitionName.stringValue());
            }
        }
        return new RefreshTableAnalyzedStatement(indexNames);
    }
}
