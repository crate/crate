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

import static io.crate.role.Privilege.Clazz.CLUSTER;
import static io.crate.role.Privilege.Clazz.SCHEMA;
import static io.crate.role.Privilege.Clazz.TABLE;
import static io.crate.role.Privilege.Clazz.VIEW;
import static io.crate.role.Privilege.State.DENY;
import static io.crate.role.Privilege.State.GRANT;
import static io.crate.role.Privilege.State.REVOKE;
import static io.crate.role.Privilege.Type.AL;
import static io.crate.role.Privilege.Type.DDL;
import static io.crate.role.Privilege.Type.DML;
import static io.crate.role.Privilege.Type.DQL;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.RoleManager;
import io.crate.role.StubRoleManager;
import io.crate.role.metadata.RolesHelper;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class PrivilegesAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final Role GRANTOR_TEST_USER = RolesHelper.userOf("test");

    private static final RoleManager ROLE_MANAGER = new StubRoleManager();

    private SQLExecutor e;

    @Before
    public void setUpSQLExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T2_DEFINITION)
            .addTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .addTable("create table my_schema.locations (id int)")
            .addView(new RelationName("my_schema", "locations_view"),
                     "select * from my_schema.locations limit 2")
            .setUserManager(ROLE_MANAGER)
            .build();
    }

    @Test
    public void testGrantPrivilegesToUsersOnCluster() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT DQL, DML TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, CLUSTER, null),
            privilegeOf(GRANT, DML, CLUSTER, null)
        );
    }

    @Test
    public void testDenyPrivilegesToUsers() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("DENY DQL, DML TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(DENY, DQL, CLUSTER, null),
            privilegeOf(DENY, DML, CLUSTER, null)
        );
    }

    public void testGrantPrivilegesToUsersOnSchemas() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT DQL, DML on schema doc, sys TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, SCHEMA, "doc"),
            privilegeOf(GRANT, DML, SCHEMA, "doc"),
            privilegeOf(GRANT, DQL, SCHEMA, "sys"),
            privilegeOf(GRANT, DML, SCHEMA, "sys")
        );
    }

    @Test
    public void testGrantPrivilegesToUsersOnTables() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT DQL, DML on table t2, locations TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, TABLE, "doc.t2"),
            privilegeOf(GRANT, DML, TABLE, "doc.t2"),
            privilegeOf(GRANT, DQL, TABLE, "doc.locations"),
            privilegeOf(GRANT, DML, TABLE, "doc.locations")
        );
    }

    @Test
    public void testGrantPrivilegesToUsersOnViews() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT DQL, DML on table t2, locations TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, TABLE, "doc.t2"),
            privilegeOf(GRANT, DML, TABLE, "doc.t2"),
            privilegeOf(GRANT, DQL, TABLE, "doc.locations"),
            privilegeOf(GRANT, DML, TABLE, "doc.locations")
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnCluster() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("REVOKE DQL, DML FROM user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, CLUSTER, null),
            privilegeOf(REVOKE, DML, CLUSTER, null)
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnSchemas() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("REVOKE DQL, DML On schema doc, sys FROM user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, SCHEMA, "doc"),
            privilegeOf(REVOKE, DML, SCHEMA, "doc"),
            privilegeOf(REVOKE, DQL, SCHEMA, "sys"),
            privilegeOf(REVOKE, DML, SCHEMA, "sys")
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnTables() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("REVOKE DQL, DML On table doc.t2, locations FROM user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, TABLE, "doc.t2"),
            privilegeOf(REVOKE, DML, TABLE, "doc.t2"),
            privilegeOf(REVOKE, DQL, TABLE, "doc.locations"),
            privilegeOf(REVOKE, DML, TABLE, "doc.locations")
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnViews() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("REVOKE DQL, DML On view doc.t2, locations FROM user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");;
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, VIEW, "doc.t2"),
            privilegeOf(REVOKE, DML, VIEW, "doc.t2"),
            privilegeOf(REVOKE, DQL, VIEW, "doc.locations"),
            privilegeOf(REVOKE, DML, VIEW, "doc.locations")
        );
    }

    @Test
    public void testGrantDenyRevokeAllPrivileges() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES TO user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, CLUSTER, null),
            privilegeOf(GRANT, DML, CLUSTER, null),
            privilegeOf(GRANT, DDL, CLUSTER, null),
            privilegeOf(GRANT, AL, CLUSTER, null)
        );

        analysis = analyzePrivilegesStatement("DENY ALL PRIVILEGES TO user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(DENY, DQL, CLUSTER, null),
            privilegeOf(DENY, DML, CLUSTER, null),
            privilegeOf(DENY, DDL, CLUSTER, null),
            privilegeOf(DENY, AL, CLUSTER, null)
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES FROM user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, CLUSTER, null),
            privilegeOf(REVOKE, DML, CLUSTER, null),
            privilegeOf(REVOKE, DDL, CLUSTER, null),
            privilegeOf(REVOKE, AL, CLUSTER, null)
        );
    }

    @Test
    public void testGrantRevokeAllPrivilegesOnSchema() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES ON Schema doc TO user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, SCHEMA, "doc"),
            privilegeOf(GRANT, DML, SCHEMA, "doc"),
            privilegeOf(GRANT, AL, SCHEMA, "doc"),
            privilegeOf(GRANT, DDL, SCHEMA, "doc")
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES ON Schema doc FROM user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, SCHEMA, "doc"),
            privilegeOf(REVOKE, DML, SCHEMA, "doc"),
            privilegeOf(REVOKE, AL, SCHEMA, "doc"),
            privilegeOf(REVOKE, DDL, SCHEMA, "doc")
        );
    }

    @Test
    public void testGrantRevokeAllPrivilegesOnTable() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES On table my_schema.locations TO user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, TABLE, "my_schema.locations"),
            privilegeOf(GRANT, DML, TABLE, "my_schema.locations"),
            privilegeOf(GRANT, AL, TABLE, "my_schema.locations"),
            privilegeOf(GRANT, DDL, TABLE, "my_schema.locations")
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES On table my_schema.locations FROM user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, TABLE, "my_schema.locations"),
            privilegeOf(REVOKE, DML, TABLE, "my_schema.locations"),
            privilegeOf(REVOKE, AL, TABLE, "my_schema.locations"),
            privilegeOf(REVOKE, DDL, TABLE, "my_schema.locations")
        );
    }

    @Test
    public void testGrantRevokeAllPrivilegesOnViews() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT ALL PRIVILEGES On view my_schema.locations_view TO user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, VIEW, "my_schema.locations_view"),
            privilegeOf(GRANT, DML, VIEW, "my_schema.locations_view"),
            privilegeOf(GRANT, AL, VIEW, "my_schema.locations_view"),
            privilegeOf(GRANT, DDL, VIEW, "my_schema.locations_view")
        );

        analysis = analyzePrivilegesStatement("REVOKE ALL PRIVILEGES On view my_schema.locations_view FROM user1");
        assertThat(analysis.privileges()).hasSize(4);
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, VIEW, "my_schema.locations_view"),
            privilegeOf(REVOKE, DML, VIEW, "my_schema.locations_view"),
            privilegeOf(REVOKE, AL, VIEW, "my_schema.locations_view"),
            privilegeOf(REVOKE, DDL, VIEW, "my_schema.locations_view")
        );
    }

    private AnalyzedPrivileges analyzePrivilegesStatement(String statement) {
        return (AnalyzedPrivileges) e.analyzer.analyze(
            SqlParser.createStatement(statement),
            new CoordinatorSessionSettings(GRANTOR_TEST_USER),
            ParamTypeHints.EMPTY,
            e.cursors
        );
    }

    @Test
    public void testGrantOnUnknownSchemaDoesntThrowsException() {
        analyzePrivilegesStatement("GRANT DQL on schema hoichi TO user1");
    }

    @Test
    public void testRevokeFromUnknownTableDoNotThrowException() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("Revoke DQL on table doc.hoichi FROM user1");
        assertThat(analysis.privileges()).hasSize(1);
        assertThat(analysis.privileges()).containsExactly(
            privilegeOf(REVOKE, DQL, TABLE, "doc.hoichi")
        );
    }

    @Test
    public void testGrantToUnknownTableThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("Grant DQL on table doc.hoichi to user1"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'doc.hoichi' unknown");
    }

    @Test
    public void testGrantToUnknownViewThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("Grant DQL on view doc.hoichi to user1"))
            .isExactlyInstanceOf(RelationUnknown.class)
            .hasMessage("Relation 'doc.hoichi' unknown");
    }

    @Test
    public void testGrantOnInformationSchemaTableThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT DQL ON TABLE information_schema.tables TO user1"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
    }

    @Test
    public void testRevokeOnInformationSchemaTableDoNotThrowException() {
        analyzePrivilegesStatement("REVOKE DQL ON TABLE information_schema.tables FROM user1");
    }


    @Test
    public void testRevokeOnInformationSchemaViewDoNotThrowException() {
        analyzePrivilegesStatement("REVOKE DQL ON TABLE information_schema.views FROM user1");
    }

    @Test
    public void testDenyOnInformationSchemaTableThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("DENY DQL ON TABLE information_schema.tables TO user1"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
    }

    @Test
    public void testDenyOnInformationSchemaViewThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("DENY DQL ON TABLE information_schema.views TO user1"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
    }

    @Test
    public void testGrantOnInformationSchemaThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT DQL ON SCHEMA information_schema TO user1"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
    }

    @Test
    public void testDenyOnInformationSchemaThrowsException() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("DENY DQL ON SCHEMA information_schema TO user1"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("GRANT/DENY/REVOKE Privileges on information_schema is not supported");
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
