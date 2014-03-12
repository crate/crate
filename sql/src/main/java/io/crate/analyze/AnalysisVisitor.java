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

    public R process(Analysis analysis, @Nullable C context) {
        return analysis.accept(this, context);
    }

    protected R visitAnalysis(Analysis analysis, C context) {
        return null;
    }

    protected R visitCopyAnalysis(CopyAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitCreateTableAnalysis(CreateTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    protected R visitDeleteAnalysis(DeleteAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitInsertAnalysis(InsertAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitSelectAnalysis(SelectAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitUpdateAnalysis(UpdateAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitDropTableAnalysis(DropTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    protected R visitCreateAnalyzerAnalysis(CreateAnalyzerAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    protected R visitDataAnalysis(AbstractDataAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    protected R visitDDLAnalysis(AbstractDDLAnalysis analysis, C context) {
        return visitAnalysis(analysis, context);
    }

    public R visitCreateBlobTableAnalysis(CreateBlobTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitDropBlobTableAnalysis(DropBlobTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitRefreshTableAnalysis(RefreshTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitAlterTableAnalysis(AlterTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }

    public R visitAlterBlobTableAnalysis(AlterBlobTableAnalysis analysis, C context) {
        return visitDDLAnalysis(analysis, context);
    }
}
