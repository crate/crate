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

package io.crate.auth.user;

import io.crate.metadata.UserDefinitions;
import io.crate.metadata.UsersMetadata;
import io.crate.metadata.UsersPrivilegesMetadata;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;

public class TransportUserActionTest extends CrateUnitTest {

    @Test
    public void testCreateFirstUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateUserAction.putUser(mdBuilder, "root", null);
        UsersMetadata metadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        assertThat(metadata.userNames().size(), is(1));
        assertThat(metadata.userNames().get(0), is("root"));
    }

    @Test
    public void testEmptyPrivilegesAreCreatedForNewUsers() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateUserAction.putUser(mdBuilder, "root", null);
        UsersPrivilegesMetadata metadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        assertThat(metadata.getUserPrivileges("root"), is(Collections.emptySet()));
    }

    @Test
    public void testCreateUserAlreadyExists() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        assertThat(TransportCreateUserAction.putUser(mdBuilder, "Arthur", null), is(true));
    }

    @Test
    public void testCreateUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        TransportCreateUserAction.putUser(mdBuilder, "Trillian", null);
        UsersMetadata newMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        assertThat(newMetadata.userNames(), containsInAnyOrder("Trillian", "Arthur"));
    }

    @Test
    public void testDropUserNoUsersAtAll() throws Exception {
        assertThat(TransportDropUserAction.dropUser(Metadata.builder(), null, "root"), is(false));
    }

    @Test
    public void testDropNonExistingUser() throws Exception {
        boolean res = TransportDropUserAction.dropUser(
                Metadata.builder(),
                new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY),
                "trillian"
        );
        assertThat(res, is(false));
    }

    @Test
    public void testDropUser() throws Exception {
        UsersMetadata oldMetadata = new UsersMetadata(UserDefinitions.DUMMY_USERS);
        Metadata.Builder mdBuilder = Metadata.builder();
        boolean res = TransportDropUserAction.dropUser(mdBuilder, oldMetadata, "Arthur");
        assertThat(users(mdBuilder), contains("Ford"));
        assertThat(res, is(true));
    }

    private static List<String> users(Metadata.Builder mdBuilder) {
        return ((UsersMetadata)mdBuilder.build().custom(UsersMetadata.TYPE)).userNames();
    }
}
