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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.SessionContext;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RelationName;
import io.crate.user.Privilege;
import io.crate.user.User;

public class AccessControlMaySeeTest extends ESTestCase {

    private List<List<Object>> validationCallArguments;
    private User user;
    private AccessControl accessControl;

    @Before
    public void setUpUserAndValidator() {
        validationCallArguments = new ArrayList<>();
        user = new User("normal", Set.of(), Set.of(), null) {

            @Override
            public boolean hasAnyPrivilege(Privilege.Clazz clazz, String ident) {
                validationCallArguments.add(Lists.newArrayList(clazz, ident, user.name()));
                return true;
            }
        };
        accessControl = new AccessControlImpl(userName -> user, new SessionContext(user));
    }

    @SuppressWarnings("unchecked")
    private void assertAskedAnyForCluster() {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(Privilege.Clazz.CLUSTER, null, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedAnyForSchema(String ident) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(Privilege.Clazz.SCHEMA, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedAnyForTable(String ident) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(Privilege.Clazz.TABLE, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @Test
    public void testTableScopeException() throws Exception {
        accessControl.ensureMaySee(new RelationValidationException(Lists.newArrayList(
            RelationName.fromIndexName("users"),
            RelationName.fromIndexName("my_schema.foo")
        ), "bla"));
        assertAskedAnyForTable("doc.users");
        assertAskedAnyForTable("my_schema.foo");
    }

    @Test
    public void testSchemaScopeException() throws Exception {
        accessControl.ensureMaySee(new SchemaUnknownException("my_schema"));
        assertAskedAnyForSchema("my_schema");
    }

    @Test
    public void testClusterScopeException() throws Exception {
        accessControl.ensureMaySee(new UnsupportedFeatureException("unsupported"));
        assertAskedAnyForCluster();
    }

    @Test
    public void testUnscopedException() throws Exception {
        accessControl.ensureMaySee(new UnhandledServerException("unhandled"));
        assertThat(validationCallArguments.size(), is(0));
    }
}
