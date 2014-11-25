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

import io.crate.PartitionName;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;

import javax.annotation.Nullable;
import java.util.Locale;

public class RefreshTableAnalyzedStatement extends AbstractDDLAnalyzedStatement {

    private final ReferenceInfos referenceInfos;
    private TableInfo tableInfo;
    private PartitionName partitionName;

    protected RefreshTableAnalyzedStatement(ReferenceInfos referenceInfos,
                                            ParameterContext parameterContext) {
        super(parameterContext);
        this.referenceInfos = referenceInfos;
    }

    @Override
    public void table(TableIdent tableIdent) {
        SchemaInfo schemaInfo = referenceInfos.getSchemaInfo(tableIdent.schema());
        if (schemaInfo == null) {
            throw new SchemaUnknownException(tableIdent.schema());
        }
        TableInfo tableInfo = schemaInfo.getTableInfo(tableIdent.name());
        if (tableInfo == null) {
            throw new TableUnknownException(tableIdent.name());
        }
        this.tableInfo = tableInfo;
    }

    public TableInfo table() {
        return tableInfo;
    }

    public @Nullable PartitionName partitionName() {
        return partitionName;
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitRefreshTableStatement(this, context);
    }

    public void partitionIdent(String ident) {
        assert table() != null;

        if (!table().isPartitioned()) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH,
                            "Table '%s' is not partitioned", table().ident().name()));
        }
        try {
            this.partitionName = PartitionName.fromPartitionIdent(
                    table().ident().name(),
                    ident
            );
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid partition ident for table '%s': '%s'",
                            table().ident().name(), ident), e);
        }

        if (!table().partitions().contains(this.partitionName)) {
            throw new PartitionUnknownException(
                    this.table().ident().name(),
                    this.partitionName.ident());
        }
    }
}
