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

package io.crate.user;

import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.user.metadata.UserDefinitions;
import io.crate.user.metadata.UsersMetadata;
import io.crate.user.metadata.UsersPrivilegesMetadata;

public class TransportUserActionTest extends ESTestCase {

    @Test
    public void testCreateFirstUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateUserAction.putUser(mdBuilder, "root", null);
        UsersMetadata metadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        assertThat(metadata.userNames()).containsExactly("root");
    }

    @Test
    public void testEmptyPrivilegesAreCreatedForNewUsers() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder();
        TransportCreateUserAction.putUser(mdBuilder, "root", null);
        UsersPrivilegesMetadata metadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        assertThat(metadata.getUserPrivileges("root")).isEmpty();
    }

    @Test
    public void testCreateUserAlreadyExists() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        assertThat(TransportCreateUserAction.putUser(mdBuilder, "Arthur", null)).isTrue();
    }

    @Test
    public void testCreateUser() throws Exception {
        Metadata.Builder mdBuilder = new Metadata.Builder()
            .putCustom(UsersMetadata.TYPE, new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY));
        TransportCreateUserAction.putUser(mdBuilder, "Trillian", null);
        UsersMetadata newMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        assertThat(newMetadata.userNames()).containsExactlyInAnyOrder("Trillian", "Arthur");
    }

    @Test
    public void testDropUserNoUsersAtAll() throws Exception {
        assertThat(TransportDropUserAction.dropUser(Metadata.builder(), null, "root")).isFalse();
    }

    @Test
    public void testDropNonExistingUser() throws Exception {
        boolean res = TransportDropUserAction.dropUser(
                Metadata.builder(),
                new UsersMetadata(UserDefinitions.SINGLE_USER_ONLY),
                "trillian"
        );
        assertThat(res).isFalse();
    }

    @Test
    public void testDropUser() throws Exception {
        UsersMetadata oldMetadata = new UsersMetadata(UserDefinitions.DUMMY_USERS);
        Metadata.Builder mdBuilder = Metadata.builder();
        boolean res = TransportDropUserAction.dropUser(mdBuilder, oldMetadata, "Arthur");
        assertThat(users(mdBuilder)).containsExactly("Ford");
        assertThat(res).isTrue();
    }

    private static List<String> users(Metadata.Builder mdBuilder) {
        return ((UsersMetadata)mdBuilder.build().custom(UsersMetadata.TYPE)).userNames();
    }
}
