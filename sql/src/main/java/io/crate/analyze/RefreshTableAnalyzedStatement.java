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

import io.crate.exceptions.PartitionUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;

import javax.annotation.Nullable;
import java.util.*;

public class RefreshTableAnalyzedStatement extends AbstractDDLAnalyzedStatement {

    private final ReferenceInfos referenceInfos;
    private final Set<TableInfo> tableInfos = new HashSet<>();
    private final Map<TableInfo, PartitionName> partitions = new HashMap<>();

    protected RefreshTableAnalyzedStatement(ReferenceInfos referenceInfos){
        this.referenceInfos = referenceInfos;
    }

    public TableInfo table(TableIdent tableIdent) {
        TableInfo tableInfo = referenceInfos.getWritableTable(tableIdent);
        tableInfos.add(tableInfo);
        return tableInfo;
    }

    public Set<TableInfo> tables() {
        return tableInfos;
    }

    public Map<TableInfo, PartitionName> partitions() {
        return partitions;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitRefreshTableStatement(this, context);
    }

    public void partitionIdent(TableInfo tableInfo, String ident) {
        assert tableInfo != null;
        PartitionName partitionName;

        if (!tableInfo.isPartitioned()) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH,
                            "Table '%s' is not partitioned", tableInfo.ident().fqn()));
        }
        try {
            partitionName = PartitionName.fromPartitionIdent(
                    tableInfo.ident().schema(),
                    tableInfo.ident().name(),
                    ident
            );

            partitions.put(tableInfo, partitionName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid partition ident for table '%s': '%s'",
                            tableInfo.ident().fqn(), ident), e);
        }

        if (!tableInfo.partitions().contains(partitionName)) {
            throw new PartitionUnknownException(
                    tableInfo.ident().fqn(),
                    partitionName.ident());
        }
    }
}
