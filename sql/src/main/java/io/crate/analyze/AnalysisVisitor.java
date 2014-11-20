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

import javax.annotation.Nullable;

public class AnalysisVisitor<C, R> {

    public R process(AnalyzedStatement analyzedStatement, @Nullable C context) {
        return analyzedStatement.accept(this, context);
    }

    protected R visitAnalysis(AnalyzedStatement analyzedStatement, C context) {
        return null;
    }

    protected R visitCopyAnalysis(CopyAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitCreateTableAnalysis(CreateTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    protected R visitDeleteAnalysis(DeleteAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitInsertFromValuesAnalysis(InsertFromValuesAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitInsertFromSubQueryAnalysis(InsertFromSubQueryAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitSelectAnalysis(SelectAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitUpdateAnalysis(UpdateAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitDropTableAnalysis(DropTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    protected R visitCreateAnalyzerAnalysis(CreateAnalyzerAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    protected R visitDataAnalysis(AbstractDataAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitDDLAnalysis(AbstractDDLAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    public R visitCreateBlobTableAnalysis(CreateBlobTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitDropBlobTableAnalysis(DropBlobTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitRefreshTableAnalysis(RefreshTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitAlterTableAnalysis(AlterTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitAlterBlobTableAnalysis(AlterBlobTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitSetAnalysis(SetAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    public R visitAddColumnAnalysis(AddColumnAnalyzedStatement analysis, C context) {
        return visitAnalysis(analysis, context);
    }
}
