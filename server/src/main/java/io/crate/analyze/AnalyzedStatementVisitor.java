/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.replication.logical.analyze.AnalyzedAlterPublication;
import io.crate.replication.logical.analyze.AnalyzedAlterSubscription;
import io.crate.replication.logical.analyze.AnalyzedCreatePublication;
import io.crate.replication.logical.analyze.AnalyzedCreateSubscription;
import io.crate.replication.logical.analyze.AnalyzedDropPublication;
import io.crate.replication.logical.analyze.AnalyzedDropSubscription;

public class AnalyzedStatementVisitor<C, R> {

    protected R visitAnalyzedStatement(AnalyzedStatement analyzedStatement, C context) {
        return null;
    }

    protected R visitCopyFromStatement(AnalyzedCopyFrom analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitCopyToStatement(AnalyzedCopyTo analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitCreateRepositoryAnalyzedStatement(AnalyzedCreateRepository analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    protected R visitAnalyzedInsertStatement(AnalyzedInsertStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitSelectStatement(AnalyzedRelation relation, C context) {
        return visitAnalyzedStatement(relation, context);
    }

    protected R visitCreateFunction(AnalyzedCreateFunction analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitDropFunction(AnalyzedDropFunction analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitAnalyzedCreateUser(AnalyzedCreateUser analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitDropUser(AnalyzedDropUser analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitCreateAnalyzerStatement(AnalyzedCreateAnalyzer analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    protected R visitDropAnalyzerStatement(AnalyzedDropAnalyzer analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    protected R visitDDLStatement(DDLStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitAnalyzedCreateBlobTable(AnalyzedCreateBlobTable analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitOptimizeTableStatement(AnalyzedOptimizeTable analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitRefreshTableStatement(AnalyzedRefreshTable analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitAnalyzedAlterTableRename(AnalyzedAlterTableRename analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitRerouteRetryFailedStatement(AnalyzedRerouteRetryFailed analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitAnalyzedAlterBlobTable(AnalyzedAlterBlobTable analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitAnalyzedAlterUser(AnalyzedAlterUser analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitSetStatement(AnalyzedSetStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitSetLicenseStatement(AnalyzedSetLicenseStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitSetSessionAuthorizationStatement(AnalyzedSetSessionAuthorizationStatement analysis,
                                                   C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitAnalyzedAlterTableOpenClose(AnalyzedAlterTableOpenClose analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitKillAnalyzedStatement(AnalyzedKill analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitDeallocateAnalyzedStatement(AnalyzedDeallocate analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitShowCreateTableAnalyzedStatement(AnalyzedShowCreateTable analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    public R visitCreateTableAs(AnalyzedCreateTableAs createTableAs, C context) {
        return visitAnalyzedStatement(createTableAs, context);
    }

    public R visitDropRepositoryAnalyzedStatement(AnalyzedDropRepository analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitDropSnapshotAnalyzedStatement(AnalyzedDropSnapshot analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitCreateSnapshotAnalyzedStatement(AnalyzedCreateSnapshot analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitRestoreSnapshotAnalyzedStatement(AnalyzedRestoreSnapshot analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    public R visitResetAnalyzedStatement(AnalyzedResetStatement resetAnalyzedStatement, C context) {
        return visitAnalyzedStatement(resetAnalyzedStatement, context);
    }

    public R visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, C context) {
        return visitAnalyzedStatement(explainAnalyzedStatement, context);
    }

    public R visitBegin(AnalyzedBegin analyzedBegin, C context) {
        return visitAnalyzedStatement(analyzedBegin, context);
    }

    public R visitCommit(AnalyzedCommit analyzedCommit, C context) {
        return visitAnalyzedStatement(analyzedCommit, context);
    }

    public R visitPrivilegesStatement(AnalyzedPrivileges analysis, C context) {
        return visitDCLStatement(analysis, context);
    }

    public R visitDCLStatement(DCLStatement analysis, C context) {
        return visitAnalyzedStatement(analysis, context);
    }

    protected R visitRerouteMoveShard(AnalyzedRerouteMoveShard analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    protected R visitRerouteAllocateReplicaShard(AnalyzedRerouteAllocateReplicaShard analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    protected R visitRerouteCancelShard(AnalyzedRerouteCancelShard analysis, C context) {
        return visitDDLStatement(analysis, context);
    }

    protected R visitAnalyzedDeleteStatement(AnalyzedDeleteStatement statement, C context) {
        return visitAnalyzedStatement(statement, context);
    }

    public R visitAnalyzedUpdateStatement(AnalyzedUpdateStatement statement, C context) {
        return visitAnalyzedStatement(statement, context);
    }

    public R visitCreateViewStmt(CreateViewStmt createViewStmt, C context) {
        return visitAnalyzedStatement(createViewStmt, context);
    }

    public R visitDropView(AnalyzedDropView dropView, C context) {
        return visitAnalyzedStatement(dropView, context);
    }

    public R visitSwapTable(AnalyzedSwapTable swapTable, C context) {
        return visitAnalyzedStatement(swapTable, context);
    }

    public R visitGCDanglingArtifacts(AnalyzedGCDanglingArtifacts gcDanglingIndices, C context) {
        return visitAnalyzedStatement(gcDanglingIndices, context);
    }

    public R visitDecommissionNode(AnalyzedDecommissionNode decommissionNode, C context) {
        return visitAnalyzedStatement(decommissionNode, context);
    }

    public R visitReroutePromoteReplica(AnalyzedPromoteReplica promoteReplicaStatement, C context) {
        return visitDDLStatement(promoteReplicaStatement, context);
    }

    public R visitDropTable(AnalyzedDropTable<?> dropTable, C context) {
        return visitDDLStatement(dropTable, context);
    }

    public R visitCreateTable(AnalyzedCreateTable createTable, C context) {
        return visitDDLStatement(createTable, context);
    }

    public R visitAlterTableAddColumn(AnalyzedAlterTableAddColumn alterTableAddColumn, C context) {
        return visitDDLStatement(alterTableAddColumn, context);
    }

    public R visitAlterTableDropCheckConstraint(AnalyzedAlterTableDropCheckConstraint dropCheckConstraint, C context) {
        return visitDDLStatement(dropCheckConstraint, context);
    }

    public R visitAlterTable(AnalyzedAlterTable alterTable, C context) {
        return visitDDLStatement(alterTable, context);
    }

    public R visitAnalyze(AnalyzedAnalyze analyzedAnalyze, C context) {
        return visitDDLStatement(analyzedAnalyze, context);
    }

    public R visitDiscard(AnalyzedDiscard discard, C context) {
        return visitAnalyzedStatement(discard, context);
    }

    public R visitSetTransaction(AnalyzedSetTransaction setTransaction, C context) {
        return visitAnalyzedStatement(setTransaction, context);
    }

    public R visitCreatePublication(AnalyzedCreatePublication createPublication, C context) {
        return visitDDLStatement(createPublication, context);
    }

    public R visitDropPublication(AnalyzedDropPublication dropPublication, C context) {
        return visitDDLStatement(dropPublication, context);
    }

    public R visitAlterPublication(AnalyzedAlterPublication alterPublication, C context) {
        return visitDDLStatement(alterPublication, context);
    }

    public R visitCreateSubscription(AnalyzedCreateSubscription createSubscription, C context) {
        return visitDDLStatement(createSubscription, context);
    }

    public R visitDropSubscription(AnalyzedDropSubscription dropSubscription, C context) {
        return visitDDLStatement(dropSubscription, context);
    }

    public R visitAlterSubscription(AnalyzedAlterSubscription alterSubscription, C context) {
        return visitDDLStatement(alterSubscription, context);
    }
}
