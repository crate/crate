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

import static io.crate.role.Permission.AL;
import static io.crate.role.Permission.DDL;
import static io.crate.role.Permission.DML;
import static io.crate.role.Permission.DQL;
import static io.crate.role.Policy.DENY;
import static io.crate.role.Policy.GRANT;
import static io.crate.role.Policy.REVOKE;
import static io.crate.role.Securable.CLUSTER;
import static io.crate.role.Securable.SCHEMA;
import static io.crate.role.Securable.TABLE;
import static io.crate.role.Securable.VIEW;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.role.Permission;
import io.crate.role.Policy;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Securable;
import io.crate.role.metadata.RolesHelper;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class PrivilegesAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private static final Role GRANTOR_TEST_USER = RolesHelper.userOf("test");


    private SQLExecutor e;

    @Before
    public void setUpSQLExecutor() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T2_DEFINITION)
            .addTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_DEFINITION)
            .addTable("create table my_schema.locations (id int)")
            .addView(new RelationName("my_schema", "locations_view"),
                     "select * from my_schema.locations limit 2");
    }

    @Test
    public void testGrantPrivilegesToUsersOnCluster() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT DQL, DML TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(GRANT, DQL, CLUSTER, null),
            privilegeOf(GRANT, DML, CLUSTER, null)
        );
    }

    @Test
    public void testDenyPrivilegesToUsers() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("DENY DQL, DML TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(DENY, DQL, CLUSTER, null),
            privilegeOf(DENY, DML, CLUSTER, null)
        );
    }

    public void testGrantPrivilegesToUsersOnSchemas() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT DQL, DML on schema doc, sys TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
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
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
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
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
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
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, CLUSTER, null),
            privilegeOf(REVOKE, DML, CLUSTER, null)
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnSchemas() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("REVOKE DQL, DML On schema doc, sys FROM user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
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
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, TABLE, "doc.t2"),
            privilegeOf(REVOKE, DML, TABLE, "doc.t2"),
            privilegeOf(REVOKE, DQL, TABLE, "doc.locations"),
            privilegeOf(REVOKE, DML, TABLE, "doc.locations")
        );
    }

    @Test
    public void testRevokePrivilegesFromUsersOnViews() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("REVOKE DQL, DML On view my_schema.locations_view FROM user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
        assertThat(analysis.privileges()).containsExactlyInAnyOrder(
            privilegeOf(REVOKE, DQL, VIEW, "my_schema.locations_view"),
            privilegeOf(REVOKE, DML, VIEW, "my_schema.locations_view")
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

    @Test
    public void testGrantOnUnknownSchemaDoesNotThrowsException() {
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
    public void testRevokeOnInformationSchemaTableDoesNotThrowException() {
        analyzePrivilegesStatement("REVOKE DQL ON TABLE information_schema.tables FROM user1");
    }

    @Test
    public void testRevokeOnInformationSchemaViewDoesNotThrowException() {
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

    @Test
    public void test_grant_role_together_with_privileges_to_user_is_not_allowed() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT role1, role2, DML TO user1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Mixing up cluster privileges with roles is not allowed");
    }

    @Test
    public void test_deny_role_is_not_allowed() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("DENY role1 TO user1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot DENY a role");
    }

    @Test
    public void test_cannot_grant_role_to_same_role() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT role3, role2 TO role1, role2"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot grant role role2 to itself as a cycle will be created");
    }

    @Test
    public void test_cannot_grant_superuser() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT role1, crate TO role2"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot grant crate superuser, to other users or roles");
    }

    @Test
    public void test_cannot_grant_roles_to_superuser() {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT role1 TO role2, crate"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot grant roles to crate superuser");
    }

    @Test
    public void test_grant_role_to_user() {
        AnalyzedPrivileges analysis = analyzePrivilegesStatement("GRANT role1, role2, role1 TO user1, user2");
        assertThat(analysis.userNames()).containsExactly("user1", "user2");
        assertThat(analysis.rolePrivilege().grantor()).isEqualTo("test");
        assertThat(analysis.rolePrivilege().policy()).isEqualTo(GRANT);
        assertThat(analysis.rolePrivilege().roleNames()).containsExactlyInAnyOrder("role1", "role2");
    }

    @Test
    public void test_fails_if_relation_doesnt_fit_securable() throws Exception {
        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT DQL ON TABLE my_schema.locations_view TO user1"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("Expected my_schema.locations_view to be a TABLE securable but got a relation of type VIEW");

        assertThatThrownBy(() -> analyzePrivilegesStatement("GRANT DQL ON VIEW my_schema.locations TO user1"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("Expected my_schema.locations to be a VIEW securable but got a relation of type BASE_TABLE");
    }

    private AnalyzedPrivileges analyzePrivilegesStatement(String statement) {
        return (AnalyzedPrivileges) e.analyzer.analyze(
            SqlParser.createStatement(statement),
            new CoordinatorSessionSettings(GRANTOR_TEST_USER),
            ParamTypeHints.EMPTY,
            e.cursors
        );
    }

    private Privilege privilegeOf(Policy policy, Permission permission, Securable securable, String ident) {
        return new Privilege(
            policy,
            permission,
            securable,
            ident,
            GRANTOR_TEST_USER.name()
        );
    }
}
