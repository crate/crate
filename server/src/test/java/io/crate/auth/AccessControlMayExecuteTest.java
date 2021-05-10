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

import com.google.common.collect.Lists;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.ParamTypeHints;
import io.crate.user.Privilege;
import io.crate.user.User;
import io.crate.user.UserLookupService;
import io.crate.user.UserManager;
import io.crate.user.UserManagerService;
import io.crate.exceptions.UnauthorizedException;
import io.crate.execution.engine.collect.sources.SysTableRegistry;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static io.crate.user.User.CRATE_USER;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AccessControlMayExecuteTest extends CrateDummyClusterServiceUnitTest {

    private List<List<Object>> validationCallArguments;
    private User user;
    private SQLExecutor e;
    private UserManager userManager;
    private User superUser;

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

        user = new User("normal", Set.of(), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident, String defaultSchema) {
                validationCallArguments.add(Lists.newArrayList(type, clazz, ident, user.name()));
                return true;
            }
        };
        superUser = new User("crate", EnumSet.of(User.Role.SUPERUSER), Set.of(), null) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, @Nullable String ident, String defaultSchema) {
                validationCallArguments.add(Lists.newArrayList(type, clazz, ident, superUser.name()));
                return true;
            }
        };
        UserLookupService userLookupService = new UserLookupService(clusterService) {

            @Nullable
            @Override
            public User findUser(String userName) {
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
            clusterService,
            userLookupService,
            new DDLClusterStateService());

        e = SQLExecutor.builder(clusterService)
            .addBlobTable("create blob table blobs")
            .enableDefaultTables()
            .setUser(superUser)
            .addView(new RelationName("doc", "v1"), "select * from users")
            .setUserManager(userManager)
            .build();
    }

    private void analyze(String stmt) {
        analyze(stmt, user);
    }

    private void analyzeAsSuperUser(String stmt) {
        analyze(stmt, CRATE_USER);
    }

    private void analyze(String stmt, User user) {
        e.analyzer.analyze(
            SqlParser.createStatement(stmt),
            new SessionContext(user),
            ParamTypeHints.EMPTY);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedForCluster(Privilege.Type type) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(type, Privilege.Clazz.CLUSTER, null, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedForSchema(Privilege.Type type, String ident) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(type, Privilege.Clazz.SCHEMA, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedForTable(Privilege.Type type, String ident) {
        assertAskedForTable(type, ident, user);
    }

    private void assertAskedForTable(Privilege.Type type, String ident, User user) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(type, Privilege.Clazz.TABLE, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedForView(Privilege.Type type, String ident) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(type, Privilege.Clazz.VIEW, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @Test
    public void testSuperUserByPassesValidation() throws Exception {
        analyzeAsSuperUser("select * from sys.cluster");
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void testAlterOtherUsersNotAllowedAsNormalUser() {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("A regular user can use ALTER USER only on himself. " +
                                        "To modify other users superuser permissions are required");
        analyze("alter user ford set (password = 'pass')");
    }

    @Test
    public void testAlterOwnUserIsAllowed() {
        analyze("alter user normal set (password = 'pass')");
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void testOptimizeNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("User \"normal\" is not authorized to execute the statement. " +
                                        "Superuser permissions are required");
        analyze("optimize table users");
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
        assertThat(validationCallArguments.size(), is(0));
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
     * in a {@link io.crate.analyze.relations.OrderedLimitedRelation}
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
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void testAddColumn() throws Exception {
        analyze("alter table users add column foo string");
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
        assertThat(validationCallArguments.size(), is(0));
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
    public void testTableFunctionsDoNotRequireAnyPermissions() {
        analyze("select 1");
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void testPermissionCheckIsDoneOnSchemaAndTableNotOnTableAlias() {
        analyze("select * from doc.users as t");
        assertAskedForTable(Privilege.Type.DQL, "doc.users");
    }

    @Test
    public void testDecommissionRequiresSuperUserPrivileges() {
        expectedException.expectMessage("User \"normal\" is not authorized to execute the statement. " +
                                        "Superuser permissions are required");
        analyze("alter cluster decommission 'n1'");
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
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void test_any_user_can_execute_set_transaction() throws Exception {
        analyze("SET TRANSACTION READ ONLY");
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void test_set_session_user_from_superuser_to_normal_user_succeeds() {
        analyze("SET SESSION AUTHORIZATION " + user.name(), superUser);
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void test_set_session_user_from_normal_user_fails() {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(
            "User \"normal\" is not authorized to execute the statement. " +
            "Superuser permissions are required or you can set the session " +
            "authorization back to the authenticated user.");
        analyze("SET SESSION AUTHORIZATION 'someuser'", user);
    }

    @Test
    public void test_set_session_user_from_normal_user_to_superuser_fails() {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(
            "User \"normal\" is not authorized to execute the statement. " +
            "Superuser permissions are required or you can set the session " +
            "authorization back to the authenticated user.");
        analyze("SET SESSION AUTHORIZATION " + superUser.name(), user);
    }

    @Test
    public void test_set_session_user_from_normal_to_originally_authenticated_user_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("SET SESSION AUTHORIZATION " + superUser.name()),
            new SessionContext(superUser, user),
            ParamTypeHints.EMPTY);
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void test_set_session_user_from_normal_user_to_default_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("SET SESSION AUTHORIZATION DEFAULT"),
            new SessionContext(superUser, user),
            ParamTypeHints.EMPTY);
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void test_reset_session_authorization_from_normal_user_succeeds() {
        e.analyzer.analyze(
            SqlParser.createStatement("RESET SESSION AUTHORIZATION"),
            new SessionContext(superUser, user),
            ParamTypeHints.EMPTY);
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void test_create_table_as_requires_DDL_on_target_and_DQL_on_source() {
        analyze("create table target_schema.target_table as (select * from sys.cluster)");
        assertAskedForSchema(Privilege.Type.DDL, "target_schema");
        assertAskedForTable(Privilege.Type.DQL, "sys.cluster");
    }
}
