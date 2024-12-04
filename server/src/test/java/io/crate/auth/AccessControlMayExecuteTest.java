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

import static io.crate.expression.udf.UdfUnitTest.DUMMY_LANG;
import static io.crate.role.Permission.READ_WRITE_DEFINE;
import static io.crate.role.Role.CRATE_USER;
import static io.crate.role.metadata.RolesHelper.userOf;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.TableDefinitions;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnauthorizedException;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.Plan;
import io.crate.protocols.postgres.TransactionState;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.RoleManager;
import io.crate.role.RoleManagerService;
import io.crate.role.Roles;
import io.crate.role.Securable;
import io.crate.role.StubRoleManager;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;

public class AccessControlMayExecuteTest extends CrateDummyClusterServiceUnitTest {

    private List<List<Object>> validationCallArguments;
    private Role normalUser;
    private Role ddlOnlyUser;
    private SQLExecutor e;
    private RoleManager roleManager;
    private final Role superUser = Role.CRATE_USER;

    @Before
    public void setUpSQLExecutor() throws Exception {
        validationCallArguments = new ArrayList<>();
        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(
            singletonList(
                new RepositoryMetadata(
                    "my_repo",
                    "fs",
                    Settings.builder().put("location", "/tmp/my_repo").build()
            )));
        ClusterState clusterState = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(clusterService.state().metadata())
                .putCustom(RepositoriesMetadata.TYPE, repositoriesMetadata))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);

        normalUser = userOf(
                       "normal",
                       Set.of(new Privilege(Policy.GRANT,
                                            Permission.DQL,
                                            Securable.SCHEMA,
                                            "custom_schema",
                                            "crate")),
                       null);
        ddlOnlyUser = userOf("ddlOnly");

        Roles roles = new Roles() {
            @Override
            public Collection<Role> roles() {
                return List.of(superUser, normalUser, ddlOnlyUser);
            }

            @Override
            public boolean hasPrivilege(Role user, Permission type, Securable securable, @Nullable String ident) {
                validationCallArguments.add(Arrays.asList(type, securable, ident, user.name()));
                if ("ddlOnly".equals(user.name())) {
                    return Permission.DDL == type;
                }
                return true;
            }
        };

        roleManager = new RoleManagerService(
            null,
            roles,
            new DDLClusterStateService()
        );

        e = SQLExecutor.builder(clusterService)
            .setRoleManager(roleManager)
            .build()
            .addBlobTable("create blob table blobs")
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .setUser(superUser)
            .addView(new RelationName("doc", "v1"), "select * from users")
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(
                new UserDefinedFunctionMetadata(
                    "custom_schema",
                    "foo",
                    List.of(FunctionArgumentDefinition.of("i", DataTypes.INTEGER)), DataTypes.INTEGER,
                    DUMMY_LANG.name(),
                    "function foo(i) { return i; }")
            );
    }

    private void analyze(String stmt) {
        analyze(stmt, normalUser);
    }

    private void analyzeAsSuperUser(String stmt) {
        analyze(stmt, CRATE_USER);
    }

    private void analyze(String stmt, Role user) {
        e.analyzer.analyze(
            SqlParser.createStatement(stmt), new CoordinatorSessionSettings(user), ParamTypeHints.EMPTY, e.cursors);
    }

    private void assertAskedForCluster(Permission permission) {
        assertAskedForCluster(permission, normalUser);
    }

    private void assertAskedForCluster(Permission permission, Role user) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(permission, Securable.CLUSTER, null, user.name()));
    }

    private void assertAskedForSchema(Permission permission, String ident) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(permission, Securable.SCHEMA, ident, normalUser.name()));
    }

    private void assertAskedForTable(Permission permission, String ident) {
        assertAskedForTable(permission, ident, normalUser);
    }

    private void assertAskedForTable(Permission permission, String ident, Role user) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(permission, Securable.TABLE, ident, user.name()));
    }

    private void assertAskedForView(Permission permission, String ident) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(permission, Securable.VIEW, ident, normalUser.name()));
    }

    @Test
    public void testSuperUserByPassesValidation() throws Exception {
        analyzeAsSuperUser("select * from sys.cluster");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_alter_other_user_requires_al_permission() {
        analyze("alter user ford set (password = 'pass')");
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void testAlterOwnUserIsAllowed() {
        analyze("alter user normal set (password = 'pass')");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_global_statements_can_be_executed_with_al_cluster_privileges() throws Exception {
        analyze("set global stats.enabled = true");
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_reset_requires_AL_privileges() throws Exception {
        analyze("reset global stats.enabled");
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_kill_is_allowed_for_any_user() throws Exception {
        // Only kills their own statements
        analyze("kill all");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void testAlterTable() throws Exception {
        analyze("alter table users set (number_of_replicas=1)");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void testCopyFrom() throws Exception {
        analyze("copy users from 'file:///tmp'");
        assertAskedForTable(Permission.DML, "doc.users");
    }

    @Test
    public void testCopyTo() throws Exception {
        analyze("copy users to DIRECTORY '/tmp'");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testCreateTable() throws Exception {
        analyze("create table my_schema.my_table (id int)");
        assertAskedForSchema(Permission.DDL, "my_schema");
    }

    @Test
    public void testCreateBlobTable() throws Exception {
        analyze("create blob table myblobs");
        assertAskedForSchema(Permission.DDL, "blob");
    }

    @Test
    public void testCreateRepository() throws Exception {
        analyze("create repository new_repository TYPE fs with (location='/tmp', compress=True)");
        assertAskedForCluster(Permission.DDL);
    }

    @Test
    public void testDropRepository() throws Exception {
        analyze("drop repository my_repo");
        assertAskedForCluster(Permission.DDL);
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        analyze("create snapshot my_repo.my_snapshot table users");
        assertAskedForCluster(Permission.DDL);
    }

    @Test
    public void testRestoreSnapshot() throws Exception {
        analyze("restore snapshot my_repo.my_snapshot table my_table");
        assertAskedForCluster(Permission.DDL);
    }

    @Test
    public void testDropSnapshot() throws Exception {
        analyze("drop snapshot my_repo.my_snap_1");
        assertAskedForCluster(Permission.DDL);
    }

    @Test
    public void testDelete() throws Exception {
        analyze("delete from users");
        assertAskedForTable(Permission.DML, "doc.users");
    }

    @Test
    public void testInsertFromValues() throws Exception {
        analyze("insert into users (id) values (1)");
        assertAskedForTable(Permission.DML, "doc.users");
    }

    @Test
    public void testInsertFromSubquery() throws Exception {
        analyze("insert into users (id) ( select id from parted )");
        assertAskedForTable(Permission.DML, "doc.users");
        assertAskedForTable(Permission.DQL, "doc.parted");
    }

    @Test
    public void testUpdate() throws Exception {
        analyze("update users set name = 'ford' where id = 1");
        assertAskedForTable(Permission.DML, "doc.users");
    }

    @Test
    public void testSelectSingleRelation() throws Exception {
        analyze("select * from sys.cluster");
        assertAskedForTable(Permission.DQL, "sys.cluster");
    }

    @Test
    public void testSelectMultiRelation() throws Exception {
        analyze("select * from sys.cluster, users");
        assertAskedForTable(Permission.DQL, "sys.cluster");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testSelectUnion() throws Exception {
        analyze("select name from sys.cluster union all select name from users");
        assertAskedForTable(Permission.DQL, "sys.cluster");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    /**
     * Union with order by (and/or limit) results
     * in a {@link io.crate.analyze.QueriedSelectRelation}
     * which wraps the {@link io.crate.analyze.relations.UnionSelect}
     */
    @Test
    public void testSelectUnionWithOrderBy() throws Exception {
        analyze("select name from sys.cluster union all select name from users order by 1");
        assertAskedForTable(Permission.DQL, "sys.cluster");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testSelectWithSubSelect() throws Exception {
        analyze("select * from (" +
                " select users.id from users join parted on users.id = parted.id::long order by users.name limit 2" +
                ") as users_parted order by users_parted.id");
        assertAskedForTable(Permission.DQL, "doc.users");
        assertAskedForTable(Permission.DQL, "doc.parted");
    }

    @Test
    public void test_select_with_scalar_subselect_in_select() {
        analyze("SELECT 1, array(SELECT users.id FROM USERS), 2");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_where() {
        analyze("SELECT generate_series(1, 10, 1) WHERE EXISTS " +
                "(SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_order_by() {
        analyze("SELECT generate_series(1, 10, 1) ORDER BY " +
                "(SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_group_by() {
        analyze("SELECT count(*) FROM (SELECT * FROM GENERATE_SERIES(1,10,1) AS g) gs " +
                "GROUP BY gs.g + (SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_having() {
        analyze("SELECT count(*) FROM parted GROUP BY id HAVING id > " +
                "(SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Permission.DQL, "doc.parted");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testCreateFunction() throws Exception {
        analyze("create function bar()" +
                " returns long language dummy_lang AS 'function() { return 1; }'");
        assertAskedForSchema(Permission.DDL, "doc");
    }

    @Test
    public void testDropFunction() throws Exception {
        analyze("drop function bar(long, object)");
        assertAskedForSchema(Permission.DDL, "doc");
    }

    @Test
    public void testDropTable() throws Exception {
        analyze("drop table users");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void testDropBlobTable() throws Exception {
        analyze("drop blob table blobs");
        assertAskedForTable(Permission.DDL, "blob.blobs");
    }

    @Test
    public void testCreateAnalyzer() throws Exception {
        analyze("create analyzer a1 (tokenizer lowercase)");
        assertAskedForCluster(Permission.DDL);
    }

    @Test
    public void testRefresh() throws Exception {
        analyze("refresh table users, parted partition (date = 1395874800000)");
        assertAskedForTable(Permission.DQL, "doc.users");
        assertAskedForTable(Permission.DQL, "doc.parted");
    }

    @Test
    public void testRenameTable() throws Exception {
        analyze("alter table users rename to users_new");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void testAlterBlobTable() throws Exception {
        analyze("alter blob table blobs set (number_of_replicas=1)");
        assertAskedForTable(Permission.DDL, "blob.blobs");
    }

    @Test
    public void testSetSessionRequiresNoPermissions() throws Exception {
        // allowed without any permissions as it affects only the user session;
        analyze("set session foo = 'bar'");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void testAddColumn() throws Exception {
        analyze("alter table users add column foo string");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void test_drop_column() {
        analyze("alter table users drop column floats");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void test_rename_column() {
        analyze("alter table users rename column floats to flts");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void testOpenCloseTable() throws Exception {
        analyze("alter table users close");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void test_alter_table_reroute() throws Exception {
        analyze("alter table users reroute MOVE SHARD 1 FROM 'node1' TO 'node2'");
        assertAskedForTable(Permission.DDL, "doc.users");
        analyze("alter table users reroute ALLOCATE REPLICA SHARD 1 ON 'node1'");
        assertAskedForTable(Permission.DDL, "doc.users");
        analyze("alter table users reroute PROMOTE REPLICA SHARD 1 ON 'node1'");
        assertAskedForTable(Permission.DDL, "doc.users");
        analyze("alter table users reroute CANCEL SHARD 1 ON 'node1'");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void testShowTable() throws Exception {
        analyze("show create table users");
        assertAskedForTable(Permission.DQL, "doc.users");

        assertThatThrownBy(() -> analyze("show create table users1"))
            .as("Exception message must not point to `users` table, because the user has no privilege on it")
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'users1' unknown");
    }

    @Test
    public void testBeginRequiresNoPermission() throws Exception {
        // Begin is currently ignored; In other RDMS it's used to start transactions with contain
        // other statements; these other statements need to be validated
        analyze("begin");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void testExplainSelect() throws Exception {
        analyze("explain select * from users");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testExplainCopyFrom() throws Exception {
        analyze("explain copy users from 'file:///tmp'");
        assertAskedForTable(Permission.DML, "doc.users");
    }

    @Test
    public void testUserWithDDLCanCreateViewOnTablesWhereDQLPermissionsAreAvailable() {
        analyze("create view xx.v1 as select * from doc.users");
        assertAskedForSchema(Permission.DDL, "xx");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testQueryOnViewRequiresOwnerToHavePrivilegesOnInvolvedRelations() {
        analyze("select * from doc.v1");
        assertAskedForView(Permission.DQL, "doc.v1");
        assertAskedForTable(Permission.DQL, "doc.users", superUser);
    }

    @Test
    public void testDroppingAViewRequiresDDLPermissionOnView() {
        analyze("drop view doc.v1");
        assertAskedForView(Permission.DDL, "doc.v1");
    }

    @Test
    public void testFunctionsUnboundToSchemaDoNotRequireAnyPermissions() {
        analyze("select 1");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void testFunctionsBoundToSchemaRequirePermissions() {
        analyze("select * from custom_schema.foo(1)");
        assertAskedForSchema(Permission.DQL, "custom_schema");
    }

    @Test
    public void testPermissionCheckIsDoneOnSchemaAndTableNotOnTableAlias() {
        analyze("select * from doc.users as t");
        assertAskedForTable(Permission.DQL, "doc.users");
    }

    @Test
    public void testDecommissionRequiresSuperUserPrivileges() {
        assertThatThrownBy(() -> analyze("alter cluster decommission 'n1'"))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessage("User \"normal\" is not authorized to execute the statement. " +
                        "Superuser permissions are required");
    }

    @Test
    public void test_alter_cluster_reroute_retry_works_for_normal_user_with_AL_privileges() {
        analyze("alter cluster reroute retry failed", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_alter_cluster_gc_dangling_artifacts_works_for_normal_user_with_AL_privileges() {
        analyze("alter cluster gc dangling artifacts", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_alter_cluster_swap_table_works_for_normal_user_with_AL_privileges() {
        // pre-configured user has all privileges
        analyze("alter cluster swap table doc.t1 to doc.t2 with (drop_source = true)", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_alter_cluster_swap_table_works_for_normal_user_with_no_AI_with_DDL_on_both_tables() {
        analyze("alter cluster swap table doc.t1 to doc.t2 with (drop_source = true)", ddlOnlyUser);
        assertAskedForCluster(Permission.AL, ddlOnlyUser); // first checks AL and if user doesn't have it, checks both DDL-s
        assertAskedForTable(Permission.DDL, "doc.t2", ddlOnlyUser);
        assertAskedForTable(Permission.DDL, "doc.t1", ddlOnlyUser);
    }

    @Test
    public void test_a_user_with_al_on_cluster_privileges_can_create_other_users() {
        analyze("create user joe");
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_a_user_with_al_on_cluster_can_delete_users() {
        analyze("drop user joe");
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_a_user_with_al_on_cluster_can_grant_privileges_he_has_to_other_users() {
        analyze("GRANT DQL ON SCHEMA foo TO joe");
        assertAskedForCluster(Permission.AL);
        assertAskedForSchema(Permission.DQL, "foo");
    }

    @Test
    public void test_a_user_with_al_can_revoke_privileges_from_users() {
        analyze("REVOKE DQL ON SCHEMA foo FROM joe");
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_any_user_can_execute_discard() throws Exception {
        analyze("discard all");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_any_user_can_execute_set_transaction() throws Exception {
        analyze("SET TRANSACTION READ ONLY");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_session_user_from_superuser_to_normal_user_succeeds() {
        analyze("SET SESSION AUTHORIZATION " + normalUser.name(), superUser);
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_session_user_from_normal_user_fails() {
        assertThatThrownBy(() -> analyze("SET SESSION AUTHORIZATION 'someuser'", normalUser))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessage("User \"normal\" is not authorized to execute the statement. " +
                        "Superuser permissions are required or you can set the session " +
                        "authorization back to the authenticated user.");
    }

    @Test
    public void test_set_session_user_from_normal_user_to_superuser_fails() {
        String stmt = "SET SESSION AUTHORIZATION " + superUser.name();
        assertThatThrownBy(() -> analyze(stmt, normalUser))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessage("User \"normal\" is not authorized to execute the statement. " +
                        "Superuser permissions are required or you can set the session " +
                        "authorization back to the authenticated user.");
    }

    @Test
    public void test_set_session_user_from_normal_to_originally_authenticated_user_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("SET SESSION AUTHORIZATION " + superUser.name()),
            new CoordinatorSessionSettings(superUser, normalUser),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_session_user_from_normal_user_to_default_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("SET SESSION AUTHORIZATION DEFAULT"),
            new CoordinatorSessionSettings(superUser, normalUser),
            ParamTypeHints.EMPTY,
            e.cursors);
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_reset_session_authorization_from_normal_user_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("RESET SESSION AUTHORIZATION"),
            new CoordinatorSessionSettings(superUser, normalUser),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_create_table_as_requires_DDL_on_target_and_DQL_on_source() {
        analyze("create table target_schema.target_table as (select * from sys.cluster)");
        assertAskedForSchema(Permission.DDL, "target_schema");
        assertAskedForTable(Permission.DQL, "sys.cluster");
    }

    @Test
    public void test_optimize_table_is_allowed_with_ddl_privileges_on_table() {
        analyze("optimize table users");
        assertAskedForTable(Permission.DDL, "doc.users");
    }

    @Test
    public void test_show_transaction_isolation_does_not_require_privileges() throws Exception {
        analyze("show transaction_isolation");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_create_publication_asks_cluster_AL_and_all_for_each_table() {
        analyze("create publication pub1 FOR TABLE t1, t2", normalUser);
        assertAskedForCluster(Permission.AL);
        for (Permission permission : READ_WRITE_DEFINE) {
            assertAskedForTable(permission, "doc.t1");
            assertAskedForTable(permission, "doc.t2");
        }
    }

    @Test
    public void test_drop_publication_asks_cluster_AL() {
        e = SQLExecutor.builder(clusterService)
            .setRoleManager(roleManager)
            .build()
            .setUser(normalUser)
            .addPublication("pub1", true);
        analyze("DROP PUBLICATION pub1", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_alter_publication_asks_cluster_AL() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .setRoleManager(roleManager)
            .build()
            .setUser(normalUser)
            .addPublication("pub1", false, new RelationName("doc", "t1"));
        analyze("ALTER PUBLICATION pub1 ADD TABLE t2", normalUser);
        assertAskedForCluster(Permission.AL);
        for (Permission permission : READ_WRITE_DEFINE) {
            assertAskedForTable(permission, "doc.t2");
        }
    }

    @Test
    public void test_create_subscription_asks_cluster_AL() {
        analyze("create subscription sub1 CONNECTION 'postgresql://user@localhost/crate:5432' PUBLICATION pub1", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_drop_subscription_asks_cluster_AL() {
        e = SQLExecutor.builder(clusterService)
            .setRoleManager(roleManager)
            .build()
            .setUser(normalUser)
            .addSubscription("sub1", "pub1");
        analyze("DROP SUBSCRIPTION sub1", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_alter_subscription_asks_cluster_AL() {
        e = SQLExecutor.builder(clusterService)
            .setRoleManager(roleManager)
            .build()
            .setUser(normalUser)
            .addSubscription("sub1", "pub1");
        analyze("ALTER SUBSCRIPTION sub1 DISABLE", normalUser);

        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_anaylze_works_for_normal_user_with_AL_privileges() {
        analyze("ANALYZE", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_declare_cursor_for_non_super_users() {
        analyze("DECLARE this_cursor NO SCROLL CURSOR FOR SELECT * FROM sys.summits;", normalUser);
        assertAskedForTable(Permission.DQL, "sys.summits");
    }

    @Test
    public void test_fetch_from_cursor_is_allowed_if_user_could_declare_it() throws Exception {
        Plan plan = e.plan("declare c1 no scroll cursor for select 1");
        e.transactionState = TransactionState.IN_TRANSACTION;
        e.execute(plan);
        analyze("fetch from c1");
    }

    @Test
    public void test_close_cursor_is_allowed_if_user_could_declare_it() throws Exception {
        Plan plan = e.plan("declare c1 no scroll cursor for select 1");
        e.transactionState = TransactionState.IN_TRANSACTION;
        e.execute(plan);
        analyze("close c1");
    }


    @Test
    public void test_create_server_requires_al() throws Exception {
        analyze("create server pg foreign data wrapper jdbc", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_drop_server_requires_al() throws Exception {
        analyze("drop server pg", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_create_foreign_table_requires_al_permission() throws Exception {
        analyze("create foreign table s.tbl (x int) server pg", normalUser);
        assertAskedForTable(Permission.AL, new RelationName("s", "tbl").fqn());
    }

    @Test
    public void test_drop_foreign_table_requires_al() throws Exception {
        analyze("create foreign table s.tbl (x int) server pg", normalUser);
        assertAskedForTable(Permission.AL, new RelationName("s", "tbl").fqn());
    }

    @Test
    public void test_create_user_mapping_requires_al_permission() throws Exception {
        analyze("create user mapping for current_user server pg", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_drop_user_mapping_requires_al() throws Exception {
        analyze("drop user mapping for current_user server pg", normalUser);
        assertAskedForCluster(Permission.AL);
    }

    @Test
    public void test_checks_user_existence() {
        var e = SQLExecutor.builder(clusterService)
            // Make sure normalUser won't be found and AC is enabled
            .setRoleManager(new StubRoleManager(List.of(Role.CRATE_USER), true))
            .build();
        e.setUser(normalUser);

        // Runs on behalf of "normalUser" but we imitate via StubRoleManager that user was dropped.
        assertThatThrownBy(
            () -> e.analyze("SELECT current_user"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("User \"normal\" was dropped");
        ;
    }

}
