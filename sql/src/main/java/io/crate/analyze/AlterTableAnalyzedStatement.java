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

import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;

import javax.annotation.Nullable;

public class AlterTableAnalyzedStatement implements DDLStatement {


    private final DocTableInfo tableInfo;
    @Nullable
    private final PartitionName partitionName;
    private final TableParameter tableParameter;
    private final boolean excludePartitions;

    public AlterTableAnalyzedStatement(DocTableInfo tableInfo,
                                       PartitionName partitionName,
                                       TableParameter tableParameter,
                                       boolean excludePartitions) {
        this.tableInfo = tableInfo;
        this.partitionName = partitionName;
        this.tableParameter = tableParameter;
        this.excludePartitions = excludePartitions;
    }

    public DocTableInfo table() {
        return tableInfo;
    }

    @Nullable
    public PartitionName partitionName() {
        return partitionName;
    }

    public boolean excludePartitions() {
        return excludePartitions;
    }

    public TableParameter tableParameter() {
        return tableParameter;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitAlterTableStatement(this, context);
    }
}
