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

import io.crate.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Table;

public class AlterTableAddColumnAnalyzer extends AbstractStatementAnalyzer<Void, AddColumnAnalysis> {

    @Override
    protected Void visitNode(Node node, AddColumnAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a AlterTableAddColumn node", node));
    }

    @Override
    public Void visitAlterTableAddColumnStatement(AlterTableAddColumn node, AddColumnAnalysis context) {
        setTableAndPartitionName(node.table(), context);

        context.analyzedTableElements(TableElementsAnalyzer.analyze(
                node.tableElement(),
                context.parameters(),
                context.fulltextAnalyzerResolver()
        ));
        context.analyzedTableElements().finalizeAndValidate();
        return null;
    }

    private void setTableAndPartitionName(Table node, AddColumnAnalysis context) {
        context.table(TableIdent.of(node));
        if (!node.partitionProperties().isEmpty()) {
            PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                    context.table(),
                    node.partitionProperties(),
                    context.parameters());
            if (!context.table().partitions().contains(partitionName)) {
                throw new IllegalArgumentException("Referenced partition does not exist.");
            }
            context.partitionName(partitionName);
        }
    }
}
