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

package io.crate.operation.user;

import com.google.common.collect.Lists;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.UnauthorizedException;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.operation.collect.sources.SysTableRegistry;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.crate.operation.user.UserManagerService.CRATE_USER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class StatementPrivilegeValidatorTest extends CrateDummyClusterServiceUnitTest {

    private List<List<Object>> validationCallArguments;
    private User user;
    private SQLExecutor e;
    private UserManager userManager;

    @Before
    public void setUpSQLExecutor() throws Exception {
        validationCallArguments = new ArrayList<>();
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(
            new RepositoryMetaData(
                "my_repo",
                "fs",
                Settings.builder().put("location", "/tmp/my_repo").build()
            ));
        ClusterState clusterState = ClusterState.builder(clusterService.state())
            .metaData(MetaData.builder(clusterService.state().metaData())
                .putCustom(RepositoriesMetaData.TYPE, repositoriesMetaData))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);

        user = new User("normal", Collections.emptySet(), Collections.emptySet()) {
            @Override
            public boolean hasPrivilege(Privilege.Type type, Privilege.Clazz clazz, String ident) {
                validationCallArguments.add(Lists.newArrayList(type, clazz, ident, user.name()));
                return true;
            }
        };
        userManager = new UserManagerService(null, null,
            null, null,
            mock(SysTableRegistry.class), clusterService, new DDLClusterStateService());

        TableIdent myBlobsIdent = new TableIdent(BlobSchemaInfo.NAME, "blobs");
        e = SQLExecutor.builder(clusterService)
            .addBlobTable(TableDefinitions.createBlobTable(myBlobsIdent, clusterService))
            .enableDefaultTables()
            .build();
    }

    private void analyze(String stmt) {
        analyze(stmt, user);
    }

    private void analyzeAsSuperUser(String stmt) {
        analyze(stmt, CRATE_USER);
    }

    private void analyze(String stmt, User user) {
        e.analyzer.boundAnalyze(SqlParser.createStatement(stmt),
            new SessionContext(0, Option.NONE, null, user,
                userManager.getStatementValidator(user),
                userManager.getExceptionValidator(user)), ParameterContext.EMPTY);
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
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(type, Privilege.Clazz.TABLE, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }


    @Test
    public void testSuperUserByPassesValidation() throws Exception {
        analyzeAsSuperUser("select * from sys.cluster");
        assertThat(validationCallArguments.size(), is(0));
    }

    @Test
    public void testSelectStatementNotAllowedAsNullUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("User `null` is not authorized to execute statement");
        analyze("select * from sys.cluster", null);
    }

    @Test
    public void testCreateUserNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("create user ford");
    }

    @Test
    public void testDropUserNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("drop user ford");
    }

    @Test
    public void testPrivilegesNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("grant dql to normal");
    }

    @Test
    public void testOptimizeNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("optimize table users");
    }

    @Test
    public void testSetGlobalNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("set global stats.enabled = true");
    }

    @Test
    public void testResetNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("reset global stats.enabled");
    }

    @Test
    public void testKillNotAllowedAsNormalUser() throws Exception {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage(is("User \"normal\" is not authorized to execute statement"));
        analyze("kill all");
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
    public void testSelectWithSubSelect() throws Exception {
        analyze("select * from (" +
                " select users.id from users join parted on users.id = parted.id order by users.name limit 2" +
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
    public void testSetSession() throws Exception {
        analyze("set session foo = 'bar'");
        assertAskedForCluster(Privilege.Type.DQL);
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
    public void testBegin() throws Exception {
        analyze("begin");
        assertAskedForCluster(Privilege.Type.DQL);
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
}


