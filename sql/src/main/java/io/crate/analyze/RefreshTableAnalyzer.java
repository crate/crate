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

import io.crate.metadata.TableIdent;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.RefreshStatement;

public class RefreshTableAnalyzer extends AbstractStatementAnalyzer<Void, RefreshTableAnalysis> {

    @Override
    public Void visitRefreshStatement(RefreshStatement node, RefreshTableAnalysis context) {
        context.table(TableIdent.of(node.table()));
        if (node.table().partitionProperties().isPresent()) {
            setParitionIdent(node.table().partitionProperties().get(), context);
        }

        return null;
    }

    private void setParitionIdent(GenericProperties partitionProperties, RefreshTableAnalysis context) {
        String partitionIdent = PartitionPropertiesAnalyzer.toPartitionIdent(
                context.table(),
                partitionProperties,
                context.parameters()
        );
        context.partitionIdent(partitionIdent);
    }
}
