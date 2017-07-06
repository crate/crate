/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.operation.user.User;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.analyze.TableDefinitions.SHARD_ROUTING;
import static io.crate.analyze.user.Privilege.Clazz.CLUSTER;
import static io.crate.analyze.user.Privilege.Clazz.SCHEMA;
import static io.crate.analyze.user.Privilege.Clazz.TABLE;
import static io.crate.analyze.user.Privilege.State.DENY;
import static io.crate.analyze.user.Privilege.State.GRANT;
import static io.crate.analyze.user.Privilege.State.REVOKE;
import static io.crate.analyze.user.Privilege.Type.DDL;
import static io.crate.analyze.user.Privilege.Type.DML;
import static io.crate.analyze.user.Privilege.Type.DQL;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class PrivilegesDCLAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final User GRANTOR_TEST_USER = new User("test", Collections.emptySet(), Collections.emptySet());

    private static final TableIdent CUSTOM_SCHEMA_IDENT = new TableIdent("my_schema", "locations");
    private static final DocTableInfo CUSTOM_SCHEMA_INFO = TestingTableInfo.builder(CUSTOM_SCHEMA_IDENT, SHARD_ROUTING)
        .add("id", DataTypes.INTEGER, ImmutableList.<String>of())
        .build();
    private static final DocTableInfoFactory CUSTOM_SCHEMA_TABLE_FACTORY = new TestingDocTableInfoFactory(
        ImmutableMap.of(CUSTOM_SCHEMA_IDENT, CUSTOM_SCHEMA_INFO));

    private SQLExecutor e;

    @Before
    public void setUpSQLExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).enableDefaultTables()
            .addSchema(new DocSchemaInfo(CUSTOM_SCHEMA_IDENT.schema(), clusterService, getFunctions(),
                new UserDefinedFunctionService(clusterService), CUSTOM_SCHEMA_TABLE_FACTORY))
            .build();
    }

    @Test
    public void testGrantPrivilegesToUsersOnCluster() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT DQL, DML TO user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(GRANT, DQL, CLUSTER, null),
            privilegeOf(GRANT, DML, CLUSTER, null))
        );
    }

    @Test
    public void testDenyPrivilegesToUsers() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("DENY DQL, DML TO user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(DENY, DQL, CLUSTER, null),
            privilegeOf(DENY, DML, CLUSTER, null))
        );
    }

    public void testGrantPrivilegesToUsersOnSchemas() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT DQL, DML on schema doc, sys TO user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(GRANT, DQL, SCHEMA, "doc"),
            privilegeOf(GRANT, DML, SCHEMA, "doc"),
            privilegeOf(GRANT, DQL, SCHEMA, "sys"),
            privilegeOf(GRANT, DML, SCHEMA, "sys"))

        );
    }

    @Test
    public void testGrantPrivilegesToUsersOnTables() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT DQL, DML on table t2, locations TO user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(GRANT, DQL, TABLE, "doc.t2"),
            privilegeOf(GRANT, DML, TABLE, "doc.t2"),
            privilegeOf(GRANT, DQL, TABLE, "doc.locations"),
            privilegeOf(GRANT, DML, TABLE, "doc.locations"))
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnCluster() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("REVOKE DQL, DML FROM user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(REVOKE, DQL, CLUSTER, null),
            privilegeOf(REVOKE, DML, CLUSTER, null))
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnSchemas() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("REVOKE DQL, DML On schema doc, sys FROM user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(REVOKE, DQL, SCHEMA, "doc"),
            privilegeOf(REVOKE, DML, SCHEMA, "doc"),
            privilegeOf(REVOKE, DQL, SCHEMA, "sys"),
            privilegeOf(REVOKE, DML, SCHEMA, "sys"))
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnTables() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("REVOKE DQL, DML On table doc.t2, locations FROM user1, user2");
        assertThat(analysis.userNames(), contains("user1", "user2"));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(REVOKE, DQL, TABLE, "doc.t2"),
            privilegeOf(REVOKE, DML, TABLE, "doc.t2"),
            privilegeOf(REVOKE, DQL, TABLE, "doc.locations"),
            privilegeOf(REVOKE, DML, TABLE, "doc.locations"))
        );
    }

    @Test
    public void testGrantDenyRevokeAllPrivileges() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES TO user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(GRANT, DQL, CLUSTER, null),
            privilegeOf(GRANT, DML, CLUSTER, null),
            privilegeOf(GRANT, DDL, CLUSTER, null))
        );

        analysis = analyzePrivilegesStatement("DENY ALL PRIVILEGES TO user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(DENY, DQL, CLUSTER, null),
            privilegeOf(DENY, DML, CLUSTER, null),
            privilegeOf(DENY, DDL, CLUSTER, null))
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES FROM user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(REVOKE, DQL, CLUSTER, null),
            privilegeOf(REVOKE, DML, CLUSTER, null),
            privilegeOf(REVOKE, DDL, CLUSTER, null))
        );
    }

    @Test
    public void testGrantRevokeAllPrivilegesOnSchema() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES ON Schema doc TO user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(GRANT, DQL, SCHEMA, "doc"),
            privilegeOf(GRANT, DML, SCHEMA, "doc"),
            privilegeOf(GRANT, DDL, SCHEMA, "doc"))
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES ON Schema doc FROM user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(REVOKE, DQL, SCHEMA, "doc"),
            privilegeOf(REVOKE, DML, SCHEMA, "doc"),
            privilegeOf(REVOKE, DDL, SCHEMA, "doc"))
        );
    }

    @Test
    public void testGrantRevokeAllPrivilegesOnTable() {
        PrivilegesAnalyzedStatement analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES On table my_schema.locations TO user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(GRANT, DQL, TABLE, "my_schema.locations"),
            privilegeOf(GRANT, DML, TABLE, "my_schema.locations"),
            privilegeOf(GRANT, DDL, TABLE, "my_schema.locations"))
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES On table my_schema.locations FROM user1");
        assertThat(analysis.privileges().size(), is(3));
        assertThat(analysis.privileges(), containsInAnyOrder(
            privilegeOf(REVOKE, DQL, TABLE, "my_schema.locations"),
            privilegeOf(REVOKE, DML, TABLE, "my_schema.locations"),
            privilegeOf(REVOKE, DDL, TABLE, "my_schema.locations"))
        );

    }

    private PrivilegesAnalyzedStatement analyzePrivilegesStatement(String statement) {
        return (PrivilegesAnalyzedStatement) e.analyzer.boundAnalyze(
            SqlParser.createStatement(statement),
            new SessionContext(0, Option.NONE, null, GRANTOR_TEST_USER, s -> {}, t -> {}), null
        ).analyzedStatement();
    }

    @Test
    public void testGrantWithoutUserManagementEnabledThrowsException() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("User management is not enabled");
        e = SQLExecutor.builder(clusterService).build();
        e.analyze("GRANT DQL TO test");
    }

    @Test
    public void testGrantOnUnknownSchemaThrowsException() {
        expectedException.expect(SchemaUnknownException.class);
        expectedException.expectMessage("Schema 'hoichi' unknown");
        analyzePrivilegesStatement("GRANT DQL on schema hoichi TO user1");
    }

    @Test
    public void testRevokeFromUnknownTableThrowsException() {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.hoichi' unknown");
        analyzePrivilegesStatement("Revoke DQL on table doc.hoichi FROM user1");
    }

    @Test
    public void testGrantOnInformationSchemaTableThrowsException() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
        analyzePrivilegesStatement("GRANT DQL ON TABLE information_schema.tables TO user1");
    }

    @Test
    public void testRevokeOnInformationSchemaTableThrowsException() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
        analyzePrivilegesStatement("REVOKE DQL ON TABLE information_schema.tables FROM user1");
    }

    @Test
    public void testDenyOnInformationSchemaTableThrowsException() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
        analyzePrivilegesStatement("DENY DQL ON TABLE information_schema.tables TO user1");
    }

    @Test
    public void testRevokeOnInformationSchemaThrowsException() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
        analyzePrivilegesStatement("REVOKE DQL ON SCHEMA information_schema FROM user1");
        analyzePrivilegesStatement("GRANT DQL ON SCHEMA information_schema TO user1");
        analyzePrivilegesStatement("DENY DQL ON SCHEMA information_schema TO user1");
    }

    @Test
    public void testGrantOnInformationSchemaThrowsException() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
        analyzePrivilegesStatement("GRANT DQL ON SCHEMA information_schema TO user1");
    }

    @Test
    public void testDenyOnInformationSchemaThrowsException() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
        analyzePrivilegesStatement("DENY DQL ON SCHEMA information_schema TO user1");
    }

    private Privilege privilegeOf(Privilege.State state, Privilege.Type type, Privilege.Clazz clazz, String ident) {
        return new Privilege(state,
            type,
            clazz,
            ident,
            GRANTOR_TEST_USER.name()
        );
    }
}
