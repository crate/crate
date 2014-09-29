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
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

public class CreateBlobTableAnalysis extends AbstractDDLAnalysis {

    private final ImmutableSettings.Builder indexSettingsBuilder = ImmutableSettings.builder();

    private Settings builtSettings;

    public CreateBlobTableAnalysis(Analyzer.ParameterContext parameterContext) {
        super(parameterContext);
    }

    @Override
    public TableInfo table() {
        return null;
    }

    @Override
    public void normalize() {
    }

    public String tableName() {
        return tableIdent.name();
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
        return analysisVisitor.visitCreateBlobTableAnalysis(this, context);
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }
}
