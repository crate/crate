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

import io.crate.analyze.user.Privilege;
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class TransportPrivilegesActionTest extends CrateUnitTest {

    private static final Privilege GRANT_DQL =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate");
    private static final Privilege GRANT_DML =
        new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate");

    private static final Set<Privilege> PRIVILEGES = new HashSet<>(Arrays.asList(GRANT_DQL, GRANT_DML));
    private static final List<String> USERNAMES = Arrays.asList("Ford", "Arthur");

    private UsersPrivilegesMetaData setUpDefaultUsersPrivilegesMetaData() {
        Map<String, Set<Privilege>> usersPrivileges = new HashMap<>();
        for (String userName : USERNAMES) {
            usersPrivileges.put(userName, new HashSet<>(PRIVILEGES));
        }
        return new UsersPrivilegesMetaData(usersPrivileges);
    }

    @Test
    public void testApplyPrivilegesWithoutAnyUsersPrivilegesMetaData() throws Exception {
        MetaData.Builder mdBuilder = MetaData.builder();
        PrivilegesRequest request = new PrivilegesRequest(USERNAMES, PRIVILEGES);
        long rowCount = TransportPrivilegesAction.applyPrivileges(mdBuilder, null, request);
        assertThat(rowCount, is(4L));

        UsersPrivilegesMetaData privilegesMetaData = (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE);
        assertThat(privilegesMetaData, is(notNullValue()));
        for (String userName : USERNAMES) {
            Set<Privilege> userPrivileges = privilegesMetaData.getUserPrivileges(userName);
            assertThat(userPrivileges, is(notNullValue()));
            assertThat(userPrivileges, is(PRIVILEGES));
        }
    }

    @Test
    public void testApplyPrivilegesSameExists() throws Exception {
        UsersPrivilegesMetaData oldMetaData = setUpDefaultUsersPrivilegesMetaData();
        MetaData.Builder mdBuilder = MetaData.builder();
        PrivilegesRequest request = new PrivilegesRequest(USERNAMES, PRIVILEGES);

        long rowCount = TransportPrivilegesAction.applyPrivileges(mdBuilder, oldMetaData, request);
        assertThat(rowCount, is(0L));
    }

    @Test
    public void testRevokeWithoutGrant() throws Exception {
        MetaData.Builder mdBuilder = MetaData.builder();
        PrivilegesRequest request = new PrivilegesRequest(Arrays.asList("Arthur"), Arrays.asList(
            new Privilege(Privilege.State.REVOKE, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate")));

        long rowCount = TransportPrivilegesAction.applyPrivileges(mdBuilder, null, request);
        assertThat(rowCount, is(0L));
        UsersPrivilegesMetaData privilegesMetaData = (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE);
        assertThat(privilegesMetaData.getUserPrivileges("Arthur"), is(nullValue()));
    }

    @Test
    public void testRevokeWithGrant() throws Exception {
        UsersPrivilegesMetaData oldMetaData = setUpDefaultUsersPrivilegesMetaData();
        MetaData.Builder mdBuilder = MetaData.builder();
        PrivilegesRequest request = new PrivilegesRequest(Arrays.asList("Arthur"), Arrays.asList(
            new Privilege(Privilege.State.REVOKE, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate")));

        long rowCount = TransportPrivilegesAction.applyPrivileges(mdBuilder, oldMetaData, request);
        assertThat(rowCount, is(1L));
        UsersPrivilegesMetaData privilegesMetaData = (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE);
        assertThat(privilegesMetaData.getUserPrivileges("Arthur"), contains(GRANT_DQL));
    }

    @Test
    public void testRevokeWithGrantOfDifferentGrantor() throws Exception {
        UsersPrivilegesMetaData oldMetaData = setUpDefaultUsersPrivilegesMetaData();
        MetaData.Builder mdBuilder = MetaData.builder();
        PrivilegesRequest request = new PrivilegesRequest(Arrays.asList("Arthur"), Arrays.asList(
            new Privilege(Privilege.State.REVOKE, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "hoschi")));

        long rowCount = TransportPrivilegesAction.applyPrivileges(mdBuilder, oldMetaData, request);
        assertThat(rowCount, is(1L));
        UsersPrivilegesMetaData privilegesMetaData = (UsersPrivilegesMetaData) mdBuilder.getCustom(UsersPrivilegesMetaData.TYPE);
        assertThat(privilegesMetaData.getUserPrivileges("Arthur"), contains(GRANT_DQL));
    }
}
