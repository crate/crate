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

package io.crate.auth;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedAlterBlobTable;
import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedAlterTableDropCheckConstraint;
import io.crate.analyze.AnalyzedAlterTableOpenClose;
import io.crate.analyze.AnalyzedAlterTableRename;
import io.crate.analyze.AnalyzedAlterUser;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedCommit;
import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.AnalyzedCopyTo;
import io.crate.analyze.AnalyzedCreateAnalyzer;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateFunction;
import io.crate.analyze.AnalyzedCreateRepository;
import io.crate.analyze.AnalyzedCreateSnapshot;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedCreateTableAs;
import io.crate.analyze.AnalyzedCreateUser;
import io.crate.analyze.AnalyzedDeallocate;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.AnalyzedDiscard;
import io.crate.analyze.AnalyzedDropFunction;
import io.crate.analyze.AnalyzedDropRepository;
import io.crate.analyze.AnalyzedDropSnapshot;
import io.crate.analyze.AnalyzedDropTable;
import io.crate.analyze.AnalyzedDropUser;
import io.crate.analyze.AnalyzedDropView;
import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedKill;
import io.crate.analyze.AnalyzedPrivileges;
import io.crate.analyze.AnalyzedRefreshTable;
import io.crate.analyze.AnalyzedResetStatement;
import io.crate.analyze.AnalyzedRestoreSnapshot;
import io.crate.analyze.AnalyzedSetSessionAuthorizationStatement;
import io.crate.analyze.AnalyzedSetStatement;
import io.crate.analyze.AnalyzedSetTransaction;
import io.crate.analyze.AnalyzedShowCreateTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.AnalyzedUpdateStatement;
import io.crate.analyze.CreateViewStmt;
import io.crate.analyze.ExplainAnalyzedStatement;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.user.Privilege;
import io.crate.user.Privileges;
import io.crate.exceptions.ClusterScopeException;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.CrateExceptionVisitor;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.SchemaScopeException;
import io.crate.exceptions.TableScopeException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.exceptions.UnscopedException;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.SetStatement;
import io.crate.user.User;
import io.crate.user.UserLookup;

import java.util.Locale;

public final class AccessControlImpl implements AccessControl {

    private final User sessionUser;
    private final User authenticatedUser;
    private final UserLookup userLookup;
    private final SessionContext sessionContext;

    /**
     * @param sessionContext for user and defaultSchema information.
     *                       The `sessionContext` (instead of user and schema) is required to
     *                       observe updates to the default schema.
     *                       (Which can change at runtime within the life-time of a session)
     */
    public AccessControlImpl(UserLookup userLookup, SessionContext sessionContext) {
        this.userLookup = userLookup;
        this.sessionContext = sessionContext;
        this.sessionUser = sessionContext.sessionUser();
        this.authenticatedUser = sessionContext.authenticatedUser();
    }

    @Override
    public void ensureMayExecute(AnalyzedStatement statement) {
        if (!sessionUser.isSuperUser()) {
            statement.accept(
                new StatementVisitor(
                    userLookup,
                    sessionContext.searchPath().currentSchema(),
                    authenticatedUser
                ),
                sessionUser
            );
        }
    }

    @Override
    public void ensureMaySee(Throwable t) throws MissingPrivilegeException {
        if (!sessionUser.isSuperUser() && t instanceof CrateException) {
            ((CrateException) t).accept(MaskSensitiveExceptions.INSTANCE, sessionUser);
        }
    }

    private static void throwRequiresSuperUserPermission(String userName) {
        throw new UnauthorizedException(
            String.format(Locale.ENGLISH, "User \"%s\" is not authorized to execute the statement. " +
                                          "Superuser permissions are required", userName));
    }

    private static class RelationContext {

        private User user;
        private final Privilege.Type type;

        RelationContext(User user, Privilege.Type type) {
            this.user = user;
            this.type = type;
        }
    }

    private static final class RelationVisitor extends AnalyzedRelationVisitor<RelationContext, Void> {

        private final UserLookup userLookup;
        private final String defaultSchema;

        public RelationVisitor(UserLookup userLookup, String defaultSchema) {
            this.userLookup = userLookup;
            this.defaultSchema = defaultSchema;
        }

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, RelationContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", relation));
        }

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, RelationContext context) {
            unionSelect.left().accept(this, context);
            unionSelect.right().accept(this, context);
            return null;
        }

        @Override
        public Void visitTableRelation(TableRelation tableRelation, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.TABLE,
                tableRelation.tableInfo().ident().fqn(),
                context.user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitDocTableRelation(DocTableRelation relation, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.TABLE,
                relation.tableInfo().ident().fqn(),
                context.user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, RelationContext context) {
            // Any user can execute table functions; Queries like `select 1` might be used to do simple connection checks
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, RelationContext context) {
            for (var source : relation.from()) {
                source.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitView(AnalyzedView analyzedView, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                context.type,
                Privilege.Clazz.VIEW,
                analyzedView.name().toString(),
                context.user,
                defaultSchema);
            User owner = analyzedView.owner() == null ? null : userLookup.findUser(analyzedView.owner());
            if (owner == null) {
                throw new UnauthorizedException(
                    "Owner \"" + analyzedView.owner() + "\" of the view \"" + analyzedView.name().fqn() + "\" not found");
            }
            User currentUser = context.user;
            context.user = owner;
            analyzedView.relation().accept(this, context);
            context.user = currentUser;
            return null;
        }
    }

    private static final class StatementVisitor extends AnalyzedStatementVisitor<User, Void> {

        private final RelationVisitor relationVisitor;
        private final String defaultSchema;
        private final User authenticatedUser;

        public StatementVisitor(UserLookup userLookup, String defaultSchema, User authenticatedUser) {
            this.authenticatedUser = authenticatedUser;
            this.relationVisitor = new RelationVisitor(userLookup, defaultSchema);
            this.defaultSchema = defaultSchema;
        }

        private void visitRelation(AnalyzedRelation relation, User user, Privilege.Type type) {
            relation.accept(relationVisitor, new RelationContext(user, type));
        }

        @Override
        protected Void visitAnalyzedStatement(AnalyzedStatement analyzedStatement, User user) {
            throwRequiresSuperUserPermission(user.name());
            return null;
        }

        @Override
        public Void visitAnalyzedAlterUser(AnalyzedAlterUser analysis, User user) {
            // user is allowed to change it's own properties
            if (!analysis.userName().equals(user.name())) {
                throw new UnauthorizedException("A regular user can use ALTER USER only on himself. " +
                                                "To modify other users superuser permissions are required.");
            }
            return null;
        }

        @Override
        public Void visitAlterTable(AnalyzedAlterTable alterTable, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                alterTable.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        protected Void visitCopyFromStatement(AnalyzedCopyFrom analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DML,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        protected Void visitCopyToStatement(AnalyzedCopyTo analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DQL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().fqn(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitCreateTable(AnalyzedCreateTable createTable, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                createTable.relationName().schema(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitCreateTableAs(AnalyzedCreateTableAs createTableAs, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                createTableAs.analyzedCreateTable().relationName().schema(),
                user,
                defaultSchema
            );
            visitRelation(createTableAs.sourceRelation(), user, Privilege.Type.DQL);
            return null;
        }

        @Override
        protected Void visitCreateRepositoryAnalyzedStatement(AnalyzedCreateRepository analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema);
            return null;
        }

        @Override
        protected Void visitAnalyzedDeleteStatement(AnalyzedDeleteStatement delete, User user) {
            visitRelation(delete.relation(), user, Privilege.Type.DML);
            return null;
        }

        @Override
        protected Void visitAnalyzedInsertStatement(AnalyzedInsertStatement analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DML,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user,
                defaultSchema);
            visitRelation(analysis.subQueryRelation(), user, Privilege.Type.DQL);
            return null;
        }

        @Override
        public Void visitSelectStatement(AnalyzedRelation relation, User user) {
            visitRelation(relation, user, Privilege.Type.DQL);
            return null;
        }

        @Override
        public Void visitAnalyzedUpdateStatement(AnalyzedUpdateStatement update, User user) {
            visitRelation(update.table(), user, Privilege.Type.DML);
            return null;
        }

        @Override
        protected Void visitCreateFunction(AnalyzedCreateFunction analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.schema(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitDropFunction(AnalyzedDropFunction analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.schema(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitDropTable(AnalyzedDropTable<?> dropTable, User user) {
            TableInfo table = dropTable.table();
            if (table != null) {
                Privileges.ensureUserHasPrivilege(
                    Privilege.Type.DDL,
                    Privilege.Clazz.TABLE,
                    table.ident().toString(),
                    user,
                    defaultSchema);
            }
            return null;
        }

        @Override
        protected Void visitCreateAnalyzerStatement(AnalyzedCreateAnalyzer analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitAnalyzedCreateBlobTable(AnalyzedCreateBlobTable analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                analysis.relationName().schema(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(AnalyzedRefreshTable analysis, User user) {
            for (DocTableInfo tableInfo : analysis.tables().values()) {
                Privileges.ensureUserHasPrivilege(
                    Privilege.Type.DQL,
                    Privilege.Clazz.TABLE,
                    tableInfo.ident().fqn(),
                    user,
                    defaultSchema);
            }
            return null;
        }

        @Override
        public Void visitAnalyzedAlterTableRename(AnalyzedAlterTableRename analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.sourceTableInfo().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitAnalyzedAlterBlobTable(AnalyzedAlterBlobTable analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitSetStatement(AnalyzedSetStatement analysis, User user) {
            if (analysis.scope().equals(SetStatement.Scope.GLOBAL)) {
                Privileges.ensureUserHasPrivilege(
                    Privilege.Type.AL,
                    Privilege.Clazz.CLUSTER,
                    null,
                    user,
                    defaultSchema
                );
            }
            return null;
        }

        @Override
        public Void visitSetSessionAuthorizationStatement(AnalyzedSetSessionAuthorizationStatement analysis,
                                                          User sessionUser) {
            if (analysis.user() != null && !authenticatedUser.name().equals(analysis.user())) {
                throw new UnauthorizedException(String.format(
                    Locale.ENGLISH,
                    "User \"%s\" is not authorized to execute the statement. " +
                    "Superuser permissions are required or you can set the session " +
                    "authorization back to the authenticated user.", sessionUser.name()));
            }
            return null;
        }

        @Override
        public Void visitAlterTableAddColumn(AnalyzedAlterTableAddColumn analysis,
                                             User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitAlterTableDropCheckConstraint(AnalyzedAlterTableDropCheckConstraint dropCheckConstraint,
                                                       User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                dropCheckConstraint.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitAnalyzedAlterTableOpenClose(AnalyzedAlterTableOpenClose analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitKillAnalyzedStatement(AnalyzedKill analysis, User user) {
            // All users can kill their own statements.
            // If the user doesn't have privileges to kill a certain job-id the row-count will be lower or 0
            return null;
        }

        @Override
        public Void visitDeallocateAnalyzedStatement(AnalyzedDeallocate analysis, User user) {
            return null;
        }

        @Override
        public Void visitShowCreateTableAnalyzedStatement(AnalyzedShowCreateTable analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DQL,
                Privilege.Clazz.TABLE,
                analysis.tableInfo().ident().toString(),
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitDropRepositoryAnalyzedStatement(AnalyzedDropRepository analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitDropSnapshotAnalyzedStatement(AnalyzedDropSnapshot analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitCreateSnapshotAnalyzedStatement(AnalyzedCreateSnapshot analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitRestoreSnapshotAnalyzedStatement(AnalyzedRestoreSnapshot analysis, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema);
            return null;
        }

        @Override
        public Void visitResetAnalyzedStatement(AnalyzedResetStatement resetAnalyzedStatement, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.AL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema
            );
            return null;
        }

        @Override
        public Void visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, User user) {
            return explainAnalyzedStatement.statement().accept(this, user);
        }

        @Override
        public Void visitBegin(AnalyzedBegin analyzedBegin, User user) {
            return null;
        }

        @Override
        public Void visitCommit(AnalyzedCommit analyzedCommit, User user) {
            return null;
        }

        @Override
        public Void visitCreateViewStmt(CreateViewStmt createViewStmt, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.DDL,
                Privilege.Clazz.SCHEMA,
                createViewStmt.name().schema(),
                user,
                defaultSchema);
            visitRelation(createViewStmt.analyzedQuery(), user, Privilege.Type.DQL);
            return null;
        }

        @Override
        protected Void visitAnalyzedCreateUser(AnalyzedCreateUser createUser, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.AL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema
            );
            return null;
        }

        @Override
        protected Void visitDropUser(AnalyzedDropUser dropUser, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.AL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema
            );
            return null;
        }

        @Override
        public Void visitPrivilegesStatement(AnalyzedPrivileges changePrivileges, User user) {
            Privileges.ensureUserHasPrivilege(
                Privilege.Type.AL,
                Privilege.Clazz.CLUSTER,
                null,
                user,
                defaultSchema
            );
            for (Privilege privilege : changePrivileges.privileges()) {
                if (privilege.state() == Privilege.State.GRANT) {
                    Privileges.ensureUserHasPrivilege(
                        privilege.ident().type(),
                        privilege.ident().clazz(),
                        privilege.ident().ident(),
                        user,
                        defaultSchema
                    );
                }
            }
            return null;
        }

        @Override
        public Void visitDropView(AnalyzedDropView dropView, User user) {
            for (RelationName name : dropView.views()) {
                Privileges.ensureUserHasPrivilege(
                    Privilege.Type.DDL,
                    Privilege.Clazz.VIEW,
                    name.toString(),
                    user,
                    defaultSchema);
            }
            return null;
        }

        @Override
        public Void visitDiscard(AnalyzedDiscard discard, User context) {
            return null;
        }

        @Override
        public Void visitSetTransaction(AnalyzedSetTransaction setTransaction, User context) {
            return null;
        }
    }

    private static class MaskSensitiveExceptions extends CrateExceptionVisitor<User, Void> {

        private static final MaskSensitiveExceptions INSTANCE = new MaskSensitiveExceptions();

        @Override
        protected Void visitCrateException(CrateException e, User context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "CrateException '%s' not supported by privileges exception validator", e.getClass()));
        }

        @Override
        protected Void visitTableScopeException(TableScopeException e, User user) {
            for (RelationName relationName : e.getTableIdents()) {
                Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, relationName.toString(), user);
            }
            return null;
        }

        @Override
        protected Void visitSchemaScopeException(SchemaScopeException e, User context) {
            Privileges.ensureUserHasPrivilege(Privilege.Clazz.SCHEMA, e.getSchemaName(), context);
            return null;
        }

        @Override
        protected Void visitClusterScopeException(ClusterScopeException e, User context) {
            Privileges.ensureUserHasPrivilege(Privilege.Clazz.CLUSTER, null, context);
            return null;
        }

        @Override
        protected Void visitUnscopedException(UnscopedException e, User context) {
            return null;
        }
    }
}
