/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;


import io.crate.analyze.AddColumnAnalyzedStatement;
import io.crate.analyze.AlterBlobTableAnalyzedStatement;
import io.crate.analyze.AlterTableAnalyzedStatement;
import io.crate.analyze.AlterTableOpenCloseAnalyzedStatement;
import io.crate.analyze.AlterTableRenameAnalyzedStatement;
import io.crate.analyze.AlterUserAnalyzedStatement;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.analyze.CopyFromAnalyzedStatement;
import io.crate.analyze.CopyToAnalyzedStatement;
import io.crate.analyze.CreateAnalyzerAnalyzedStatement;
import io.crate.analyze.CreateBlobTableAnalyzedStatement;
import io.crate.analyze.CreateFunctionAnalyzedStatement;
import io.crate.analyze.CreateIngestionRuleAnalysedStatement;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.analyze.CreateSnapshotAnalyzedStatement;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DeallocateAnalyzedStatement;
import io.crate.analyze.DropBlobTableAnalyzedStatement;
import io.crate.analyze.DropFunctionAnalyzedStatement;
import io.crate.analyze.DropIngestionRuleAnalysedStatement;
import io.crate.analyze.DropRepositoryAnalyzedStatement;
import io.crate.analyze.DropSnapshotAnalyzedStatement;
import io.crate.analyze.DropTableAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.analyze.ExplainAnalyzedStatement;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.InsertFromValuesAnalyzedStatement;
import io.crate.analyze.KillAnalyzedStatement;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OptimizeTableAnalyzedStatement;
import io.crate.analyze.PrivilegesAnalyzedStatement;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.RefreshTableAnalyzedStatement;
import io.crate.analyze.RerouteRetryFailedAnalyzedStatement;
import io.crate.analyze.ResetAnalyzedStatement;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.analyze.SetAnalyzedStatement;
import io.crate.analyze.ShowCreateTableAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.UnauthorizedException;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.SetStatement;

import java.util.Locale;

class StatementPrivilegeValidator implements StatementAuthorizedValidator {

    private static final StatementVisitor VISITOR = new StatementVisitor();

    private final User user;

    StatementPrivilegeValidator(User user) {
        this.user = user;
    }

    @Override
    public void ensureStatementAuthorized(AnalyzedStatement statement) {
        VISITOR.process(statement, user);
    }

    private static final class StatementVisitor extends AnalyzedStatementVisitor<User, Void> {

        private static final RelationVisitor RELATION_VISITOR = new RelationVisitor();

        private static void throwUnauthorized(User user) {
            throw new UnauthorizedException(
                String.format(Locale.ENGLISH, "User \"%s\" is not authorized to execute statement", user.name()));
        }

        private void visitRelation(AnalyzedRelation relation, User user, Privilege.Type type) {
            RELATION_VISITOR.process(relation, new RelationContext(user, type));
        }


        @Override
        protected Void visitAnalyzedStatement(AnalyzedStatement analyzedStatement, User user) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement));
        }

        @Override
        protected Void visitCreateUserStatement(CreateUserAnalyzedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitAlterUserStatement(AlterUserAnalyzedStatement analysis, User user) {
            // user is allowed to change it's own properties
            if (!analysis.userName().equals(user.name())) {
                throwUnauthorized(user);
            }
            return null;
        }

        @Override
        protected Void visitDropUserStatement(DropUserAnalyzedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitPrivilegesStatement(PrivilegesAnalyzedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitCreateIngestRuleStatement(CreateIngestionRuleAnalysedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitDropIngestRuleStatement(DropIngestionRuleAnalysedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitRerouteRetryFailedStatement(RerouteRetryFailedAnalyzedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.table().ident().toString(),
                user);
            return null;
        }

        @Override
        protected Void visitCopyFromStatement(CopyFromAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DML,
                Privilege.Clazz.TABLE,
                analysis.table().ident().toString(),
                user);
            return null;
        }

        @Override
        protected Void visitCopyToStatement(CopyToAnalyzedStatement analysis, User user) {
            visitRelation(analysis.subQueryRelation(), user, Privilege.Type.DQL);
            return null;
        }

        @Override
        protected Void visitCreateTableStatement(CreateTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.tableIdent().schema(),
                user);
            return null;
        }

        @Override
        protected Void visitCreateRepositoryAnalyzedStatement(CreateRepositoryAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user);
            return null;
        }

        @Override
        protected Void visitAnalyzedDeleteStatement(AnalyzedDeleteStatement delete, User user) {
            visitRelation(delete.relation(), user, Privilege.Type.DML);
            return null;
        }

        @Override
        protected Void visitInsertFromValuesStatement(InsertFromValuesAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DML,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user);
            return null;
        }

        @Override
        protected Void visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DML,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user);
            visitRelation(analysis.subQueryRelation(), user, Privilege.Type.DQL);
            return null;
        }

        @Override
        public Void visitSelectStatement(QueriedRelation relation, User user) {
            visitRelation(relation, user, Privilege.Type.DQL);
            return null;
        }

        @Override
        public Void visitAnalyzedUpdateStatement(AnalyzedUpdateStatement update, User user) {
            visitRelation(update.table(), user, Privilege.Type.DML);
            return null;
        }

        @Override
        protected Void visitCreateFunctionStatement(CreateFunctionAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.schema(),
                user);
            return null;
        }

        @Override
        public Void visitDropFunctionStatement(DropFunctionAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.schema(),
                user);
            return null;
        }

        @Override
        protected Void visitDropTableStatement(DropTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.tableIdent().toString(),
                user);
            return null;
        }

        @Override
        protected Void visitCreateAnalyzerStatement(CreateAnalyzerAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user);
            return null;
        }

        @Override
        public Void visitCreateBlobTableStatement(CreateBlobTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.tableIdent().schema(),
                user);
            return null;
        }

        @Override
        public Void visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.tableIdent().toString(),
                user);
            return null;
        }

        @Override
        public Void visitOptimizeTableStatement(OptimizeTableAnalyzedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, User user) {
            for (String indexName : analysis.indexNames()) {
                String tableName;
                if (IndexParts.isPartitioned(indexName)) {
                    tableName = PartitionName.fromIndexOrTemplate(indexName).tableIdent().toString();
                } else {
                    tableName = TableIdent.fqnFromIndexName(indexName);
                }
                Privileges.ensureUserHasPrivilege(
                    Privilege.Type.DQL,
                    Privilege.Clazz.TABLE,
                    tableName,
                    user);
            }
            return null;
        }

        @Override
        public Void visitAlterTableRenameStatement(AlterTableRenameAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.sourceTableInfo().toString(),
                user);
            return null;
        }

        @Override
        public Void visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.table().ident().toString(),
                user);
            return null;
        }

        @Override
        public Void visitSetStatement(SetAnalyzedStatement analysis, User user) {
            if (analysis.scope().equals(SetStatement.Scope.GLOBAL)) {
                throwUnauthorized(user);
                return null;
            }
            return null;
        }

        @Override
        public Void visitAddColumnStatement(AddColumnAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.table().ident().toString(),
                user);
            return null;
        }

        @Override
        public Void visitAlterTableOpenCloseStatement(AlterTableOpenCloseAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user);
            return null;
        }

        @Override
        public Void visitKillAnalyzedStatement(KillAnalyzedStatement analysis, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitDeallocateAnalyzedStatement(DeallocateAnalyzedStatement analysis, User user) {
            return null;
        }

        @Override
        public Void visitShowCreateTableAnalyzedStatement(ShowCreateTableAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DQL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user);
            return null;
        }

        @Override
        public Void visitDropRepositoryAnalyzedStatement(DropRepositoryAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user);
            return null;
        }

        @Override
        public Void visitDropSnapshotAnalyzedStatement(DropSnapshotAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user);
            return null;
        }

        @Override
        public Void visitCreateSnapshotAnalyzedStatement(CreateSnapshotAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user);
            return null;
        }

        @Override
        public Void visitRestoreSnapshotAnalyzedStatement(RestoreSnapshotAnalyzedStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user);
            return null;
        }

        @Override
        public Void visitResetAnalyzedStatement(ResetAnalyzedStatement resetAnalyzedStatement, User user) {
            throwUnauthorized(user);
            return null;
        }

        @Override
        public Void visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, User user) {
            return process(explainAnalyzedStatement.statement(), user);
        }

        @Override
        public Void visitBegin(AnalyzedBegin analyzedBegin, User user) {
            return null;
        }
    }

    private static class RelationContext {

        private final User user;
        private final Privilege.Type type;

        RelationContext(User user, Privilege.Type type) {
            this.user = user;
            this.type = type;
        }
    }


    private static final class RelationVisitor extends AnalyzedRelationVisitor<RelationContext, Void> {

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, RelationContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", relation));
        }

        @Override
        public Void visitQueriedTable(QueriedTable table, RelationContext context) {
            process(table.tableRelation(), context);
            return null;
        }

        @Override
        public Void visitQueriedDocTable(QueriedDocTable table, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.TABLE,
                table.tableRelation().getQualifiedName().toString(),
                context.user);
            return null;
        }

        @Override
        public Void visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, RelationContext context) {
            for (AnalyzedRelation relation : multiSourceSelect.sources().values()) {
                process(relation, context);
            }
            return null;
        }

        @Override
        public Void visitOrderedLimitedRelation(OrderedLimitedRelation relation, RelationContext context) {
            process(relation.childRelation(), context);
            return null;
        }

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, RelationContext context) {
            process(unionSelect.left(), context);
            process(unionSelect.right(), context);
            return null;
        }

        @Override
        public Void visitTableRelation(TableRelation tableRelation, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.TABLE,
                tableRelation.getQualifiedName().toString(),
                context.user);
            return null;
        }

        @Override
        public Void visitDocTableRelation(DocTableRelation relation, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.TABLE,
                relation.getQualifiedName().toString(),
                context.user);
            return null;
        }

        @Override
        public Void visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.TABLE,
                tableFunctionRelation.tableInfo().ident().toString(),
                context.user);
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, RelationContext context) {
            return process(relation.subRelation(), context);
        }
    }
}
