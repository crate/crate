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
import static io.crate.user.Privilege.Type.READ_WRITE_DEFINE;
import static io.crate.user.Role.CRATE_USER;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.ClusterServiceUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.exceptions.UnauthorizedException;
import io.crate.execution.engine.collect.sources.SysTableRegistry;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.TransactionState;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import io.crate.user.Privilege;
import io.crate.user.Role;
import io.crate.user.UserLookupService;
import io.crate.user.UserManager;
import io.crate.user.UserManagerService;

public class AccessControlMayExecuteTest extends CrateDummyClusterServiceUnitTest {

    private List<List<Object>> validationCallArguments;
    private Role user;
    private SQLExecutor e;
    private UserManager userManager;
    private Role superUser;

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

        user = new Role("normal",
                        true,
                        Set.of(new Privilege(Privilege.State.GRANT,
                                             Privilege.Type.DQL,
                                             Privilege.Clazz.SCHEMA,
                                             "custom_schema",
                                             "crate")),
                        null,
            Set.of()) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident) {
                validationCallArguments.add(CollectionUtils.arrayAsArrayList(type, clazz, ident, user.name()));
                return true;
            }
        };
        superUser = new Role("crate", true, Set.of(), null, EnumSet.of(Role.UserRole.SUPERUSER)) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, @Nullable String ident) {
                validationCallArguments.add(CollectionUtils.arrayAsArrayList(type, clazz, ident, superUser.name()));
                return true;
            }
        };
        UserLookupService userLookupService = new UserLookupService(clusterService) {

            @Nullable
            @Override
            public Role findUser(String userName) {
                if ("crate".equals(userName)) {
                    return superUser;
                }
                return super.findUser(userName);
            }
        };
        userManager = new UserManagerService(
            null,
            null,
            null,
            null,
            mock(SysTableRegistry.class),
            userLookupService,
            new DDLClusterStateService());

        e = SQLExecutor.builder(clusterService)
            .addBlobTable("create blob table blobs")
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .setUser(superUser)
            .addView(new RelationName("doc", "v1"), "select * from users")
            .setUserManager(userManager)
            .addUDFLanguage(DUMMY_LANG)
            .addUDF(
                new UserDefinedFunctionMetadata(
                    "custom_schema",
                    "foo",
                    List.of(FunctionArgumentDefinition.of("i", DataTypes.INTEGER)), DataTypes.INTEGER,
                    DUMMY_LANG.name(),
                    "function foo(i) { return i; }")
            )
            .build();
    }

    private void executePlan(Plan plan) {
        TestingRowConsumer consumer = new TestingRowConsumer(true);
        plan.execute(
            mock(DependencyCarrier.class, Answers.RETURNS_MOCKS),
            e.getPlannerContext(clusterService.state()),
            consumer,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
    }

    private void analyze(String stmt) {
        analyze(stmt, user);
    }

    private void analyzeAsSuperUser(String stmt) {
        analyze(stmt, CRATE_USER);
    }

    private void analyze(String stmt, Role user) {
        e.analyzer.analyze(
            SqlParser.createStatement(stmt), new CoordinatorSessionSettings(user), ParamTypeHints.EMPTY, e.cursors);
    }

    private void assertAskedForCluster(Privilege.Type type) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(type, Privilege.Clazz.CLUSTER, null, user.name()));
    }

    private void assertAskedForSchema(Privilege.Type type, String ident) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(type, Privilege.Clazz.SCHEMA, ident, user.name()));
    }

    private void assertAskedForTable(Privilege.Type type, String ident) {
        assertAskedForTable(type, ident, user);
    }

    private void assertAskedForTable(Privilege.Type type, String ident, Role user) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(type, Privilege.Clazz.TABLE, ident, user.name()));
    }

    private void assertAskedForView(Privilege.Type type, String ident) {
        assertThat(validationCallArguments).anySatisfy(
            s -> assertThat(s).containsExactly(type, Privilege.Clazz.VIEW, ident, user.name()));
    }

    @Test
    public void testSuperUserByPassesValidation() throws Exception {
        analyzeAsSuperUser("select * from sys.cluster");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void testAlterOtherUsersNotAllowedAsNormalUser() {
        assertThatThrownBy(() -> analyze("alter user ford set (password = 'pass')"))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessage("A regular user can use ALTER USER only on himself. " +
                        "To modify other users superuser permissions are required.");
    }

    @Test
    public void testAlterOwnUserIsAllowed() {
        analyze("alter user normal set (password = 'pass')");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_global_statements_can_be_executed_with_al_cluster_privileges() throws Exception {
        analyze("set global stats.enabled = true");
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_reset_requires_AL_privileges() throws Exception {
        analyze("reset global stats.enabled");
        assertAskedForCluster(Privilege.Type.AL);
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
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void testCopyFrom() throws Exception {
        analyze("copy users from 'file:///tmp'");
        assertAskedForTable(Privilege.Type.DML, "doc.users");
    }

    @Test
    public void testCopyTo() throws Exception {
        analyze("copy users to DIRECTORY '/tmp'");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testCreateTable() throws Exception {
        analyze("create table my_schema.my_table (id int)");
        assertAskedForSchema(Privilege.Type.DDL, "my_schema");
    }

    @Test
    public void testCreateBlobTable() throws Exception {
        analyze("create blob table myblobs");
        assertAskedForSchema(Privilege.Type.DDL, "blob");
    }

    @Test
    public void testCreateRepository() throws Exception {
        analyze("create repository new_repository TYPE fs with (location='/tmp', compress=True)");
        assertAskedForCluster(Privilege.Type.DDL);
    }

    @Test
    public void testDropRepository() throws Exception {
        analyze("drop repository my_repo");
        assertAskedForCluster(Privilege.Type.DDL);
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        analyze("create snapshot my_repo.my_snapshot table users");
        assertAskedForCluster(Privilege.Type.DDL);
    }

    @Test
    public void testRestoreSnapshot() throws Exception {
        analyze("restore snapshot my_repo.my_snapshot table my_table");
        assertAskedForCluster(Privilege.Type.DDL);
    }

    @Test
    public void testDropSnapshot() throws Exception {
        analyze("drop snapshot my_repo.my_snap_1");
        assertAskedForCluster(Privilege.Type.DDL);
    }

    @Test
    public void testDelete() throws Exception {
        analyze("delete from users");
        assertAskedForTable(Privilege.Type.DML, "doc.users");
    }

    @Test
    public void testInsertFromValues() throws Exception {
        analyze("insert into users (id) values (1)");
        assertAskedForTable(Privilege.Type.DML, "doc.users");
    }

    @Test
    public void testInsertFromSubquery() throws Exception {
        analyze("insert into users (id) ( select id from parted )");
        assertAskedForTable(Privilege.Type.DML, "doc.users");
        assertAskedForTable(Privilege.Type.DQL, "doc.parted");
    }

    @Test
    public void testUpdate() throws Exception {
        analyze("update users set name = 'ford' where id = 1");
        assertAskedForTable(Privilege.Type.DML, "doc.users");
    }

    @Test
    public void testSelectSingleRelation() throws Exception {
        analyze("select * from sys.cluster");
        assertAskedForTable(Privilege.Type.DQL, "sys.cluster");
    }

    @Test
    public void testSelectMultiRelation() throws Exception {
        analyze("select * from sys.cluster, users");
        assertAskedForTable(Privilege.Type.DQL, "sys.cluster");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testSelectUnion() throws Exception {
        analyze("select name from sys.cluster union all select name from users");
        assertAskedForTable(Privilege.Type.DQL, "sys.cluster");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    /**
     * Union with order by (and/or limit) results
     * in a {@link io.crate.analyze.QueriedSelectRelation}
     * which wraps the {@link io.crate.analyze.relations.UnionSelect}
     */
    @Test
    public void testSelectUnionWithOrderBy() throws Exception {
        analyze("select name from sys.cluster union all select name from users order by 1");
        assertAskedForTable(Privilege.Type.DQL, "sys.cluster");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testSelectWithSubSelect() throws Exception {
        analyze("select * from (" +
                " select users.id from users join parted on users.id = parted.id::long order by users.name limit 2" +
                ") as users_parted order by users_parted.id");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
        assertAskedForTable(Privilege.Type.DQL, "doc.parted");
    }

    @Test
    public void test_select_with_scalar_subselect_in_select() {
        analyze("SELECT 1, array(SELECT users.id FROM USERS), 2");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_where() {
        analyze("SELECT generate_series(1, 10, 1) WHERE EXISTS " +
                "(SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_order_by() {
        analyze("SELECT generate_series(1, 10, 1) ORDER BY " +
                "(SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_group_by() {
        analyze("SELECT count(*) FROM (SELECT * FROM GENERATE_SERIES(1,10,1) AS g) gs " +
                "GROUP BY gs.g + (SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void test_select_with_scalar_subselect_in_having() {
        analyze("SELECT count(*) FROM parted GROUP BY id HAVING id > " +
                "(SELECT u.a + 10 FROM (SELECT users.id AS a FROM USERS) u)");
        assertAskedForTable(Privilege.Type.DQL, "doc.parted");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testCreateFunction() throws Exception {
        analyze("create function bar()" +
                " returns long language dummy_lang AS 'function() { return 1; }'");
        assertAskedForSchema(Privilege.Type.DDL, "doc");
    }

    @Test
    public void testDropFunction() throws Exception {
        analyze("drop function bar(long, object)");
        assertAskedForSchema(Privilege.Type.DDL, "doc");
    }

    @Test
    public void testDropTable() throws Exception {
        analyze("drop table users");
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void testDropBlobTable() throws Exception {
        analyze("drop blob table blobs");
        assertAskedForTable(Privilege.Type.DDL, "blob.blobs");
    }

    @Test
    public void testCreateAnalyzer() throws Exception {
        analyze("create analyzer a1 (tokenizer lowercase)");
        assertAskedForCluster(Privilege.Type.DDL);
    }

    @Test
    public void testRefresh() throws Exception {
        analyze("refresh table users, parted partition (date = 1395874800000)");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
        assertAskedForTable(Privilege.Type.DQL, "doc.parted");
    }

    @Test
    public void testRenameTable() throws Exception {
        analyze("alter table users rename to users_new");
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void testAlterBlobTable() throws Exception {
        analyze("alter blob table blobs set (number_of_replicas=1)");
        assertAskedForTable(Privilege.Type.DDL, "blob.blobs");
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
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void test_drop_column() {
        analyze("alter table users drop column floats");
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void testOpenCloseTable() throws Exception {
        analyze("alter table users close");
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void testShowTable() throws Exception {
        analyze("show create table users");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
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
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testExplainCopyFrom() throws Exception {
        analyze("explain copy users from 'file:///tmp'");
        assertAskedForTable(Privilege.Type.DML, "doc.users");
    }

    @Test
    public void testUserWithDDLCanCreateViewOnTablesWhereDQLPermissionsAreAvailable() {
        analyze("create view xx.v1 as select * from doc.users");
        assertAskedForSchema(Privilege.Type.DDL, "xx");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testQueryOnViewRequiresOwnerToHavePrivilegesOnInvolvedRelations() {
        analyze("select * from doc.v1");
        assertAskedForView(Privilege.Type.DQL, "doc.v1");
        assertAskedForTable(Privilege.Type.DQL, "doc.users", superUser);
    }

    @Test
    public void testDroppingAViewRequiresDDLPermissionOnView() {
        analyze("drop view doc.v1");
        assertAskedForView(Privilege.Type.DDL, "doc.v1");
    }

    @Test
    public void testFunctionsUnboundToSchemaDoNotRequireAnyPermissions() {
        analyze("select 1");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void testFunctionsBoundToSchemaRequirePermissions() {
        analyze("select * from custom_schema.foo(1)");
        assertAskedForSchema(Privilege.Type.DQL, "custom_schema");
    }

    @Test
    public void testPermissionCheckIsDoneOnSchemaAndTableNotOnTableAlias() {
        analyze("select * from doc.users as t");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
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
        analyze("alter cluster reroute retry failed", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_alter_cluster_gc_dangling_artifacts_works_for_normal_user_with_AL_privileges() {
        analyze("alter cluster gc dangling artifacts", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_alter_cluster_swap_table_works_for_normal_user_with_AL_privileges() {
        // pre-configured user has all privileges
        analyze("alter cluster swap table doc.t1 to doc.t2 with (drop_source = true)", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_alter_cluster_swap_table_works_for_normal_user_with_no_AI_with_DDL_on_both_tables() {
        // custom user has only DML privileges
        var customUser = new Role("normal", true, Set.of(), null, Set.of()) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident) {
                validationCallArguments.add(CollectionUtils.arrayAsArrayList(type, clazz, ident, user.name()));
                return Privilege.Type.DDL == type;
            }
        };
        analyze("alter cluster swap table doc.t1 to doc.t2 with (drop_source = true)", customUser);
        assertAskedForCluster(Privilege.Type.AL); // first checks AL and if user doesn't have it, checks both DDL-s
        assertAskedForTable(Privilege.Type.DDL, "doc.t2", customUser);
        assertAskedForTable(Privilege.Type.DDL, "doc.t1", customUser);
    }

    @Test
    public void test_a_user_with_al_on_cluster_privileges_can_create_other_users() {
        analyze("create user joe");
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_a_user_with_al_on_cluster_can_delete_users() {
        analyze("drop user joe");
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_a_user_with_al_on_cluster_can_grant_privileges_he_has_to_other_users() {
        analyze("GRANT DQL ON SCHEMA foo TO joe");
        assertAskedForCluster(Privilege.Type.AL);
        assertAskedForSchema(Privilege.Type.DQL, "foo");
    }

    @Test
    public void test_a_user_with_al_can_revoke_privileges_from_users() {
        analyze("REVOKE DQL ON SCHEMA foo FROM joe");
        assertAskedForCluster(Privilege.Type.AL);
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
        analyze("SET SESSION AUTHORIZATION " + user.name(), superUser);
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_session_user_from_normal_user_fails() {
        assertThatThrownBy(() -> analyze("SET SESSION AUTHORIZATION 'someuser'", user))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessage("User \"normal\" is not authorized to execute the statement. " +
                        "Superuser permissions are required or you can set the session " +
                        "authorization back to the authenticated user.");
    }

    @Test
    public void test_set_session_user_from_normal_user_to_superuser_fails() {
        String stmt = "SET SESSION AUTHORIZATION " + superUser.name();
        assertThatThrownBy(() -> analyze(stmt, user))
            .isExactlyInstanceOf(UnauthorizedException.class)
            .hasMessage("User \"normal\" is not authorized to execute the statement. " +
                        "Superuser permissions are required or you can set the session " +
                        "authorization back to the authenticated user.");
    }

    @Test
    public void test_set_session_user_from_normal_to_originally_authenticated_user_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("SET SESSION AUTHORIZATION " + superUser.name()),
            new CoordinatorSessionSettings(superUser, user),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_set_session_user_from_normal_user_to_default_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("SET SESSION AUTHORIZATION DEFAULT"),
            new CoordinatorSessionSettings(superUser, user),
            ParamTypeHints.EMPTY,
            e.cursors);
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_reset_session_authorization_from_normal_user_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("RESET SESSION AUTHORIZATION"),
            new CoordinatorSessionSettings(superUser, user),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_create_table_as_requires_DDL_on_target_and_DQL_on_source() {
        analyze("create table target_schema.target_table as (select * from sys.cluster)");
        assertAskedForSchema(Privilege.Type.DDL, "target_schema");
        assertAskedForTable(Privilege.Type.DQL, "sys.cluster");
    }

    @Test
    public void test_optimize_table_is_allowed_with_ddl_privileges_on_table() {
        analyze("optimize table users");
        assertAskedForTable(Privilege.Type.DDL, "doc.users");
    }

    @Test
    public void test_show_transaction_isolation_does_not_require_privileges() throws Exception {
        analyze("show transaction_isolation");
        assertThat(validationCallArguments).isEmpty();
    }

    @Test
    public void test_create_publication_asks_cluster_AL_and_all_for_each_table() {
        analyze("create publication pub1 FOR TABLE t1, t2", user);
        assertAskedForCluster(Privilege.Type.AL);
        for (Privilege.Type type: READ_WRITE_DEFINE) {
            assertAskedForTable(type, "doc.t1");
            assertAskedForTable(type, "doc.t2");
        }
    }

    @Test
    public void test_drop_publication_asks_cluster_AL() {
        e = SQLExecutor.builder(clusterService)
            .setUser(user)
            .addPublication("pub1", true)
            .setUserManager(userManager)
            .build();
        analyze("DROP PUBLICATION pub1", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_alter_publication_asks_cluster_AL() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .setUser(user)
            .addPublication("pub1", false, new RelationName("doc", "t1"))
            .setUserManager(userManager)
            .build();
        analyze("ALTER PUBLICATION pub1 ADD TABLE t2", user);
        assertAskedForCluster(Privilege.Type.AL);
        for (Privilege.Type type: READ_WRITE_DEFINE) {
            assertAskedForTable(type, "doc.t2");
        }
    }

    @Test
    public void test_create_subscription_asks_cluster_AL() {
        analyze("create subscription sub1 CONNECTION 'postgresql://user@localhost/crate:5432' PUBLICATION pub1", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_drop_subscription_asks_cluster_AL() {
        e = SQLExecutor.builder(clusterService)
            .setUser(user)
            .addSubscription("sub1", "pub1")
            .setUserManager(userManager)
            .build();
        analyze("DROP SUBSCRIPTION sub1", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_alter_subscription_asks_cluster_AL() {
        e = SQLExecutor.builder(clusterService)
            .setUser(user)
            .addSubscription("sub1", "pub1")
            .setUserManager(userManager)
            .build();
        analyze("ALTER SUBSCRIPTION sub1 DISABLE", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_anaylze_works_for_normal_user_with_AL_privileges() {
        analyze("ANALYZE", user);
        assertAskedForCluster(Privilege.Type.AL);
    }

    @Test
    public void test_declare_cursor_for_non_super_users() {
        analyze("DECLARE this_cursor NO SCROLL CURSOR FOR SELECT * FROM sys.summits;", user);
        assertAskedForTable(Privilege.Type.DQL, "sys.summits");
    }

    @Test
    public void test_fetch_from_cursor_is_allowed_if_user_could_declare_it() throws Exception {
        Plan plan = e.plan("declare c1 no scroll cursor for select 1");
        e.transactionState = TransactionState.IN_TRANSACTION;
        executePlan(plan);
        analyze("fetch from c1");
    }

    @Test
    public void test_close_cursor_is_allowed_if_user_could_declare_it() throws Exception {
        Plan plan = e.plan("declare c1 no scroll cursor for select 1");
        e.transactionState = TransactionState.IN_TRANSACTION;
        executePlan(plan);
        analyze("close c1");
    }

}
