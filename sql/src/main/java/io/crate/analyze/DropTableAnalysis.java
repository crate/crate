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

import com.google.common.base.Joiner;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;

import javax.annotation.Nullable;

public class DropTableAnalysis extends AbstractDDLAnalysis {

    private TableInfo tableInfo;
    private final ReferenceInfos referenceInfos;

    public DropTableAnalysis(ReferenceInfos referenceInfos) {
        super(new Object[0]);
        this.referenceInfos = referenceInfos;
    }

    public String index() {
        return tableIdent.name();
    }

    @Override
    public void table(TableIdent tableIdent) {
        SchemaInfo schemaInfo = referenceInfos.getSchemaInfo(tableIdent.schema());
        if (schemaInfo == null) {
            throw new SchemaUnknownException(tableIdent.schema());
        }
        if (schemaInfo.systemSchema()) {
            throw new UnsupportedOperationException(
                    String.format("cannot delete '%s'.", Joiner.on('.').join(tableIdent.schema(), tableIdent.name())));
        }
        TableInfo tableInfo = schemaInfo.getTableInfo(tableIdent.name());
        if (tableInfo == null) {
            throw new TableUnknownException(tableIdent.name());
        } else if (tableInfo.isAlias()) {
            throw new UnsupportedOperationException("Table alias not allowed in DROP TABLE statement.");
        }
        this.tableIdent = tableIdent;
        this.tableInfo = tableInfo;
    }

    @Override
    public TableInfo table() {
        return tableInfo;
    }

    @Override
    @Nullable
    public SchemaInfo schema() {
        if (tableIdent == null) { return null; }
        return referenceInfos.getSchemaInfo(tableIdent.schema());
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitDropTableAnalysis(this, context);
    }

    @Override
    public boolean isData() {
        // TODO: remove DropTableAnalysis from Planner and extend DDLVisitor in the Transport
        return true;
    }
}
