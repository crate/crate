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

import static io.crate.role.Permission.READ_WRITE_DEFINE;

import java.util.Locale;

import io.crate.analyze.AnalyzedAlterBlobTable;
import io.crate.analyze.AnalyzedAlterRole;
import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedAlterTableDropCheckConstraint;
import io.crate.analyze.AnalyzedAlterTableDropColumn;
import io.crate.analyze.AnalyzedAlterTableOpenClose;
import io.crate.analyze.AnalyzedAlterTableRenameColumn;
import io.crate.analyze.AnalyzedAlterTableRenameTable;
import io.crate.analyze.AnalyzedAnalyze;
import io.crate.analyze.AnalyzedBegin;
import io.crate.analyze.AnalyzedClose;
import io.crate.analyze.AnalyzedCommit;
import io.crate.analyze.AnalyzedCopyFrom;
import io.crate.analyze.AnalyzedCopyTo;
import io.crate.analyze.AnalyzedCreateAnalyzer;
import io.crate.analyze.AnalyzedCreateBlobTable;
import io.crate.analyze.AnalyzedCreateFunction;
import io.crate.analyze.AnalyzedCreateRepository;
import io.crate.analyze.AnalyzedCreateRole;
import io.crate.analyze.AnalyzedCreateSnapshot;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedCreateTableAs;
import io.crate.analyze.AnalyzedDeallocate;
import io.crate.analyze.AnalyzedDeclare;
import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.AnalyzedDiscard;
import io.crate.analyze.AnalyzedDropFunction;
import io.crate.analyze.AnalyzedDropRepository;
import io.crate.analyze.AnalyzedDropRole;
import io.crate.analyze.AnalyzedDropSnapshot;
import io.crate.analyze.AnalyzedDropTable;
import io.crate.analyze.AnalyzedDropView;
import io.crate.analyze.AnalyzedFetch;
import io.crate.analyze.AnalyzedGCDanglingArtifacts;
import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedKill;
import io.crate.analyze.AnalyzedOptimizeTable;
import io.crate.analyze.AnalyzedPrivileges;
import io.crate.analyze.AnalyzedRefreshTable;
import io.crate.analyze.AnalyzedRerouteRetryFailed;
import io.crate.analyze.AnalyzedResetStatement;
import io.crate.analyze.AnalyzedRestoreSnapshot;
import io.crate.analyze.AnalyzedSetSessionAuthorizationStatement;
import io.crate.analyze.AnalyzedSetStatement;
import io.crate.analyze.AnalyzedSetTransaction;
import io.crate.analyze.AnalyzedShowCreateTable;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.AnalyzedSwapTable;
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
import io.crate.exceptions.ClusterScopeException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.CrateExceptionVisitor;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.SchemaScopeException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableScopeException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.exceptions.UnscopedException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.replication.logical.analyze.AnalyzedAlterPublication;
import io.crate.replication.logical.analyze.AnalyzedAlterSubscription;
import io.crate.replication.logical.analyze.AnalyzedCreatePublication;
import io.crate.replication.logical.analyze.AnalyzedCreateSubscription;
import io.crate.replication.logical.analyze.AnalyzedDropPublication;
import io.crate.replication.logical.analyze.AnalyzedDropSubscription;
import io.crate.role.Permission;
import io.crate.role.Privilege;
import io.crate.role.PrivilegeState;
import io.crate.role.Privileges;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.sql.tree.SetStatement;

public final class AccessControlImpl implements AccessControl {

    private final Role sessionUser;
    private final Role authenticatedUser;
    private final Roles roles;
    private final MaskSensitiveExceptions maskSensitiveExceptions;

    /**
     * @param sessionSettings for user and defaultSchema information.
     *                       The `sessionSettings` (instead of user and schema) is required to
     *                       observe updates to the default schema.
     *                       (Which can change at runtime within the life-time of a session)
     */
    public AccessControlImpl(Roles roles, CoordinatorSessionSettings sessionSettings) {
        this.roles = roles;
        this.sessionUser = sessionSettings.sessionUser();
        this.authenticatedUser = sessionSettings.authenticatedUser();
        this.maskSensitiveExceptions = new MaskSensitiveExceptions(roles);
    }

    @Override
    public void ensureMayExecute(AnalyzedStatement statement) {
        if (!sessionUser.isSuperUser()) {
            statement.accept(
                new StatementVisitor(
                    roles,
                    authenticatedUser
                ),
                sessionUser
            );
        }
    }

    @Override
    public void ensureMaySee(Throwable t) throws MissingPrivilegeException {
        if (!sessionUser.isSuperUser() && t instanceof CrateException ce) {
            ce.accept(maskSensitiveExceptions, sessionUser);
        }
    }

    private static void throwRequiresSuperUserPermission(String userName) {
        throw new UnauthorizedException(
            String.format(Locale.ENGLISH, "User \"%s\" is not authorized to execute the statement. " +
                                          "Superuser permissions are required", userName));
    }

    private static class RelationContext {

        private Role user;
        private final Permission permission;

        RelationContext(Role user, Permission permission) {
            this.user = user;
            this.permission = permission;
        }
    }

    private static final class RelationVisitor extends AnalyzedRelationVisitor<RelationContext, Void> {

        private final Roles roles;

        public RelationVisitor(Roles roles) {
            this.roles = roles;
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
                roles,
                context.user,
                context.permission,
                Securable.TABLE,
                tableRelation.tableInfo().ident().fqn()
            );
            return null;
        }

        @Override
        public Void visitDocTableRelation(DocTableRelation relation, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                roles,
                context.user,
                context.permission,
                Securable.TABLE,
                relation.tableInfo().ident().fqn()
            );
            return null;
        }

        @Override
        public Void visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, RelationContext context) {
            String schema = tableFunctionRelation.relationName().schema();

            // ex) select * from custom_schema.udf(); -- the user must have privilege for custom_schema
            if (schema != null) {
                Privileges.ensureUserHasPrivilege(
                    roles, context.user, Permission.DQL, Securable.SCHEMA, schema);
            }
            // On the other hand, all users should be able to access built-in functions without any privileges.
            // ex) select * from abs(1);
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, RelationContext context) {
            for (var source : relation.from()) {
                source.accept(this, context);
            }
            relation.visitSymbols(symbol -> {
                for (var rel : SymbolVisitors.extractAnalyzedRelations(symbol)) {
                    rel.accept(this, context);
                }
            });
            return null;
        }

        @Override
        public Void visitView(AnalyzedView analyzedView, RelationContext context) {
            Privileges.ensureUserHasPrivilege(
                roles,
                context.user,
                context.permission,
                Securable.VIEW,
                analyzedView.name().toString()
            );
            Role owner = analyzedView.owner() == null ? null : roles.findUser(analyzedView.owner());
            if (owner == null) {
                throw new UnauthorizedException(
                    "Owner \"" + analyzedView.owner() + "\" of the view \"" + analyzedView.name().fqn() + "\" not found");
            }
            Role currentUser = context.user;
            context.user = owner;
            analyzedView.relation().accept(this, context);
            context.user = currentUser;
            return null;
        }
    }

    private static final class StatementVisitor extends AnalyzedStatementVisitor<Role, Void> {

        private final RelationVisitor relationVisitor;
        private final Role authenticatedUser;

        public StatementVisitor(Roles roles, Role authenticatedUser) {
            this.authenticatedUser = authenticatedUser;
            this.relationVisitor = new RelationVisitor(roles);
        }

        private void visitRelation(AnalyzedRelation relation, Role user, Permission permission) {
            relation.accept(relationVisitor, new RelationContext(user, permission));
        }

        @Override
        protected Void visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Role user) {
            throwRequiresSuperUserPermission(user.name());
            return null;
        }

        @Override
        public Void visitDeclare(AnalyzedDeclare declare, Role user) {
            declare.query().accept(this, user);
            return null;
        }

        @Override
        public Void visitFetch(AnalyzedFetch fetch, Role user) {
            // We always allow to fetch. The privileges are checked through `Declare` when the user creates the cursor.
            return null;
        }

        @Override
        public Void visitClose(AnalyzedClose close, Role user) {
            // We always allow to close a cursor. The privileges are checked through `Declare` when the user creates
            // the cursor.
            return null;
        }

        @Override
        public Void visitSwapTable(AnalyzedSwapTable swapTable, Role user) {
            Roles roles = relationVisitor.roles;
            if (!roles.hasPrivilege(user, Permission.AL, Securable.CLUSTER, null)) {
                if (!roles.hasPrivilege(user, Permission.DDL, Securable.TABLE, swapTable.target().ident().fqn())
                    || !roles.hasPrivilege(user, Permission.DDL, Securable.TABLE, swapTable.source().ident().fqn())
                ) {
                    throw new MissingPrivilegeException(user.name());
                }
            }
            return null;
        }

        @Override
        public Void visitGCDanglingArtifacts(AnalyzedGCDanglingArtifacts gcDanglingArtifacts, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitRerouteRetryFailedStatement(AnalyzedRerouteRetryFailed rerouteRetryFailed, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitAnalyzedAlterRole(AnalyzedAlterRole analysis, Role user) {
            // user is allowed to change it's own properties
            if (!analysis.roleName().equals(user.name())) {
                throw new UnauthorizedException("A regular user can use ALTER USER only on himself. " +
                                                "To modify other users superuser permissions are required.");
            }
            return null;
        }

        @Override
        public Void visitAlterTable(AnalyzedAlterTable alterTable, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                alterTable.tableInfo().ident().toString()
            );
            return null;
        }

        @Override
        protected Void visitCopyFromStatement(AnalyzedCopyFrom analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DML,
                Securable.TABLE,
                analysis.tableInfo().ident().toString()
            );
            return null;
        }

        @Override
        protected Void visitCopyToStatement(AnalyzedCopyTo analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DQL,
                Securable.TABLE,
                analysis.tableInfo().ident().fqn()
            );
            return null;
        }

        @Override
        public Void visitCreateTable(AnalyzedCreateTable createTable, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.SCHEMA,
                createTable.relationName().schema()
            );
            return null;
        }

        @Override
        public Void visitCreateTableAs(AnalyzedCreateTableAs createTableAs, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.SCHEMA,
                createTableAs.analyzedCreateTable().relationName().schema()
            );
            visitRelation(createTableAs.sourceRelation(), user, Permission.DQL);
            return null;
        }

        @Override
        protected Void visitCreateRepositoryAnalyzedStatement(AnalyzedCreateRepository analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        protected Void visitAnalyzedDeleteStatement(AnalyzedDeleteStatement delete, Role user) {
            visitRelation(delete.relation(), user, Permission.DML);
            return null;
        }

        @Override
        protected Void visitAnalyzedInsertStatement(AnalyzedInsertStatement analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DML,
                Securable.TABLE,
                analysis.tableInfo().ident().toString()
            );
            visitRelation(analysis.subQueryRelation(), user, Permission.DQL);
            return null;
        }

        @Override
        public Void visitSelectStatement(AnalyzedRelation relation, Role user) {
            visitRelation(relation, user, Permission.DQL);
            return null;
        }

        @Override
        public Void visitAnalyzedUpdateStatement(AnalyzedUpdateStatement update, Role user) {
            visitRelation(update.table(), user, Permission.DML);
            return null;
        }

        @Override
        protected Void visitCreateFunction(AnalyzedCreateFunction analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.SCHEMA,
                analysis.schema()
            );
            return null;
        }

        @Override
        public Void visitDropFunction(AnalyzedDropFunction analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.SCHEMA,
                analysis.schema()
            );
            return null;
        }

        @Override
        public Void visitDropTable(AnalyzedDropTable<?> dropTable, Role user) {
            TableInfo table = dropTable.table();
            if (table != null) {
                Privileges.ensureUserHasPrivilege(
                    relationVisitor.roles,
                    user,
                    Permission.DDL,
                    Securable.TABLE,
                    table.ident().toString()
                );
            }
            return null;
        }

        @Override
        protected Void visitCreateAnalyzerStatement(AnalyzedCreateAnalyzer analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitAnalyzedCreateBlobTable(AnalyzedCreateBlobTable analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.SCHEMA,
                analysis.relationName().schema()
            );
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(AnalyzedRefreshTable analysis, Role user) {
            for (DocTableInfo tableInfo : analysis.tables().values()) {
                Privileges.ensureUserHasPrivilege(
                    relationVisitor.roles,
                    user,
                    Permission.DQL,
                    Securable.TABLE,
                    tableInfo.ident().fqn()
                );
            }
            return null;
        }

        @Override
        public Void visitAnalyzedAlterTableRenameTable(AnalyzedAlterTableRenameTable analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                analysis.sourceName().fqn()
            );
            return null;
        }

        @Override
        public Void visitAnalyzedAlterBlobTable(AnalyzedAlterBlobTable analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                analysis.tableInfo().ident().toString()
            );
            return null;
        }

        @Override
        public Void visitSetStatement(AnalyzedSetStatement analysis, Role user) {
            if (analysis.scope().equals(SetStatement.Scope.GLOBAL)) {
                Privileges.ensureUserHasPrivilege(
                    relationVisitor.roles,
                    user,
                    Permission.AL,
                    Securable.CLUSTER,
                    null
                );
            }
            return null;
        }

        @Override
        public Void visitSetSessionAuthorizationStatement(AnalyzedSetSessionAuthorizationStatement analysis,
                                                          Role sessionUser) {
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
        public Void visitAlterTableAddColumn(AnalyzedAlterTableAddColumn analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                analysis.table().ident().toString()
            );
            return null;
        }

        @Override
        public Void visitAlterTableDropColumn(AnalyzedAlterTableDropColumn analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                analysis.table().ident().toString()
            );
            return null;
        }

        @Override
        public Void visitAlterTableRenameColumn(AnalyzedAlterTableRenameColumn analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                analysis.table().toString()
            );
            return null;
        }

        @Override
        public Void visitAlterTableDropCheckConstraint(AnalyzedAlterTableDropCheckConstraint dropCheckConstraint,
                                                      Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                dropCheckConstraint.tableInfo().ident().toString()
            );
            return null;
        }

        @Override
        public Void visitAnalyzedAlterTableOpenClose(AnalyzedAlterTableOpenClose analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.TABLE,
                analysis.tableInfo().ident().toString()
            );
            return null;
        }

        @Override
        public Void visitKillAnalyzedStatement(AnalyzedKill analysis, Role user) {
            // All users can kill their own statements.
            // If the user doesn't have privileges to kill a certain job-id the row-count will be lower or 0
            return null;
        }

        @Override
        public Void visitDeallocateAnalyzedStatement(AnalyzedDeallocate analysis, Role user) {
            return null;
        }

        @Override
        public Void visitShowCreateTableAnalyzedStatement(AnalyzedShowCreateTable analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DQL,
                Securable.TABLE,
                analysis.tableInfo().ident().toString()
            );
            return null;
        }

        @Override
        public Void visitDropRepositoryAnalyzedStatement(AnalyzedDropRepository analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitDropSnapshotAnalyzedStatement(AnalyzedDropSnapshot analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitCreateSnapshotAnalyzedStatement(AnalyzedCreateSnapshot analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitRestoreSnapshotAnalyzedStatement(AnalyzedRestoreSnapshot analysis, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitResetAnalyzedStatement(AnalyzedResetStatement resetAnalyzedStatement, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitExplainStatement(ExplainAnalyzedStatement explainAnalyzedStatement, Role user) {
            return explainAnalyzedStatement.statement().accept(this, user);
        }

        @Override
        public Void visitBegin(AnalyzedBegin analyzedBegin, Role user) {
            return null;
        }

        @Override
        public Void visitCommit(AnalyzedCommit analyzedCommit, Role user) {
            return null;
        }

        @Override
        public Void visitCreateViewStmt(CreateViewStmt createViewStmt, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.DDL,
                Securable.SCHEMA,
                createViewStmt.name().schema()
            );
            visitRelation(createViewStmt.analyzedQuery(), user, Permission.DQL);
            return null;
        }

        @Override
        protected Void visitAnalyzedCreateRole(AnalyzedCreateRole createRole, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        protected Void visitDropRole(AnalyzedDropRole dropRole, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitPrivilegesStatement(AnalyzedPrivileges changePrivileges, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            for (Privilege privilege : changePrivileges.privileges()) {
                if (privilege.state() == PrivilegeState.GRANT) {
                    Privileges.ensureUserHasPrivilege(
                        relationVisitor.roles,
                        user,
                        privilege.ident().permission(),
                        privilege.ident().securable(),
                        privilege.ident().ident()
                    );
                }
            }
            return null;
        }

        @Override
        public Void visitDropView(AnalyzedDropView dropView, Role user) {
            for (RelationName name : dropView.views()) {
                Privileges.ensureUserHasPrivilege(
                    relationVisitor.roles,
                    user,
                    Permission.DDL,
                    Securable.VIEW,
                    name.toString()
                );
            }
            return null;
        }

        @Override
        public Void visitDiscard(AnalyzedDiscard discard, Role user) {
            return null;
        }

        @Override
        public Void visitSetTransaction(AnalyzedSetTransaction setTransaction, Role user) {
            return null;
        }

        @Override
        public Void visitOptimizeTableStatement(AnalyzedOptimizeTable optimizeTable, Role user) {
            for (TableInfo table : optimizeTable.tables().values()) {
                Privileges.ensureUserHasPrivilege(
                    relationVisitor.roles,
                    user,
                    Permission.DDL,
                    Securable.TABLE,
                    table.ident().toString()
                );
            }
            return null;
        }

        @Override
        public Void visitCreatePublication(AnalyzedCreatePublication createPublication, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            // All tables cannot be checked on publication creation - they are checked before actual replication starts
            // and a table gets published only if publication owner has DQL, DML and DDL privileges on that table.
            for (RelationName relationName: createPublication.tables()) {
                for (Permission permission : READ_WRITE_DEFINE) {
                    Privileges.ensureUserHasPrivilege(
                        relationVisitor.roles,
                        user,
                            permission,
                        Securable.TABLE,
                        relationName.fqn()
                    );
                }
            }
            return null;
        }

        @Override
        public Void visitDropPublication(AnalyzedDropPublication dropPublication, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitAlterPublication(AnalyzedAlterPublication alterPublication, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            for (RelationName relationName: alterPublication.tables()) {
                for (Permission permission : READ_WRITE_DEFINE) {
                    Privileges.ensureUserHasPrivilege(
                        relationVisitor.roles,
                        user,
                            permission,
                        Securable.TABLE,
                        relationName.fqn()
                    );
                }
            }
            return null;
        }

        @Override
        public Void visitCreateSubscription(AnalyzedCreateSubscription createSubscription, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitDropSubscription(AnalyzedDropSubscription dropSubscription, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitAlterSubscription(AnalyzedAlterSubscription alterSubscription, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }

        @Override
        public Void visitAnalyze(AnalyzedAnalyze analyzedAnalyze, Role user) {
            Privileges.ensureUserHasPrivilege(
                relationVisitor.roles,
                user,
                Permission.AL,
                Securable.CLUSTER,
                null
            );
            return null;
        }
    }

    private static class MaskSensitiveExceptions extends CrateExceptionVisitor<Role, Void> {

        private final Roles roles;

        private MaskSensitiveExceptions(Roles roles) {
            this.roles = roles;
        }

        @Override
        protected Void visitCrateException(CrateException e, Role user) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "CrateException '%s' not supported by privileges exception validator", e.getClass()));
        }

        @Override
        protected Void visitTableScopeException(TableScopeException e, Role user) {
            for (RelationName relationName : e.getTableIdents()) {
                Privileges.ensureUserHasPrivilege(roles, user, Securable.TABLE, relationName.toString());
            }
            return null;
        }

        @Override
        protected Void visitColumnUnknownException(ColumnUnknownException e, Role user) {
            if (e.relationType() == ColumnUnknownException.RelationType.TABLE_FUNCTION) {
                RelationName relationName = e.getTableIdents().iterator().next();
                if (relationName == null) { // ex) select '{"x":10}'::object['y']
                    return null;
                }
                String schema = relationName.schema();
                if (schema == null) { // ex) select unknown_col from empty_row()
                    return null;
                }
                visitSchemaScopeException(new SchemaUnknownException(schema), user);
            } else {
                visitTableScopeException(e, user);
            }
            return null;
        }

        @Override
        protected Void visitSchemaScopeException(SchemaScopeException e, Role user) {
            Privileges.ensureUserHasPrivilege(roles, user, Securable.SCHEMA, e.getSchemaName());
            return null;
        }

        @Override
        protected Void visitUnsupportedFunctionException(UnsupportedFunctionException e, Role user) {
            if (e.getSchemaName() != null) {
                visitSchemaScopeException(e, user);
            }
            return null;
        }

        @Override
        protected Void visitClusterScopeException(ClusterScopeException e, Role user) {
            Privileges.ensureUserHasPrivilege(roles, user, Securable.CLUSTER, null);
            return null;
        }

        @Override
        protected Void visitUnscopedException(UnscopedException e, Role user) {
            return null;
        }
    }
}
