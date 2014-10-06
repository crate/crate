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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

public class AlterBlobTableAnalysis extends AbstractDDLAnalysis {

    private final ImmutableSettings.Builder indexSettingsBuilder = ImmutableSettings.builder();

    private Settings builtSettings;
    private final ReferenceInfos referenceInfos;
    private TableInfo tableInfo;
    private SchemaInfo schemaInfo;

    public AlterBlobTableAnalysis(Analyzer.ParameterContext parameterContext, ReferenceInfos referenceInfos) {
        super(parameterContext);
        this.referenceInfos = referenceInfos;
    }

    @Override
    public void table(TableIdent tableIdent) {
        assert tableIdent.schema().equalsIgnoreCase(BlobSchemaInfo.NAME);
        this.tableIdent = tableIdent;

        schemaInfo = referenceInfos.getSchemaInfo(tableIdent.schema());
        assert schemaInfo != null; // schemaInfo for blob must exist.

        tableInfo = schemaInfo.getTableInfo(tableIdent.name());
        if (tableInfo == null) {
            throw new TableUnknownException(tableIdent.name());
        }
    }

    @Override
    public TableInfo table() {
        return tableInfo;
    }

    @Override
    public void normalize() {

    }

    public ImmutableSettings.Builder indexSettingsBuilder() {
        return indexSettingsBuilder;
    }

    public Settings indexSettings() {
        if (builtSettings == null) {
            builtSettings = indexSettingsBuilder.build();
        }
        return builtSettings;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitAlterBlobTableAnalysis(this, context);
    }
}
