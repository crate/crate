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

public class AnalyzedStatementVisitor<C, R> {

    public R process(AnalyzedStatement analyzedStatement, @Nullable C context) {
        return analyzedStatement.accept(this, context);
    }

    protected R visitAnalyzedStatement(AnalyzedStatement analyzedStatement, C context) {
        return null;
    }

    protected R visitCopyStatement(CopyAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitCreateTableStatement(CreateTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    protected R visitCreateRepositoryAnalyzedStatement(CreateRepositoryAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    protected R visitDeleteStatement(DeleteAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitInsertFromValuesStatement(InsertFromValuesAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitSelectStatement(SelectAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitUpdateStatement(UpdateAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitDropTableStatement(DropTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    protected R visitCreateAnalyzerStatement(CreateAnalyzerAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    protected R visitDDLAnalyzedStatement(AbstractDDLAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitCreateBlobTableStatement(CreateBlobTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitAlterTableStatement(AlterTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitSetStatement(SetAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitAddColumnStatement(AddColumnAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitKillAnalyzedStatement(KillAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitShowAnalyzedStatement(AbstractShowAnalyzedStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitShowCreateTableAnalyzedStatement(ShowCreateTableAnalyzedStatement analysis, C context) {
        return visitShowAnalyzedStatement(analysis, context);
    }

    public R visitDropRepositoryAnalyzedStatement(DropRepositoryAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitDropSnapshotAnalyzedStatement(DropSnapshotAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

    public R visitCreateSnapshotAnalyzedStatement(CreateSnapshotAnalyzedStatement analysis, C context) {
        return visitDDLAnalyzedStatement(analysis, context);
    }

}
