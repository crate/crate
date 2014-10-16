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

import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;

public class DropBlobTableAnalysis extends AbstractDDLAnalysis {

    private final ReferenceInfos referenceInfos;
    private TableInfo tableInfo;
    private SchemaInfo schemaInfo;

    protected DropBlobTableAnalysis(Analyzer.ParameterContext parameterContext, ReferenceInfos referenceInfos) {
        super(parameterContext);
        this.referenceInfos = referenceInfos;
    }

    @Override
    public void table(TableIdent tableIdent) {
        assert tableIdent.schema().equalsIgnoreCase(BlobSchemaInfo.NAME);
        this.tableIdent = tableIdent;
        schemaInfo = referenceInfos.getSchemaInfo(tableIdent.schema());
        assert schemaInfo != null; // schema info for blob must exist.

        tableInfo = schemaInfo.getTableInfo(tableIdent.name());
        if (tableInfo == null) {
            throw new TableUnknownException(tableIdent.name());
        } else if (tableInfo.isAlias()) {
            throw new UnsupportedOperationException("Table alias not allowed in DROP BLOB TABLE statement.");
        }
    }

    @Override
    public TableInfo table() {
        return tableInfo;
    }

    @Override
    public void normalize() {

    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitDropBlobTableAnalysis(this, context);
    }
}
