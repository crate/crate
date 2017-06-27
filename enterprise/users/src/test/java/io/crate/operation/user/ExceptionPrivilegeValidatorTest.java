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
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;

public class ExceptionPrivilegeValidatorTest extends CrateUnitTest {

    private List<List<Object>> validationCallArguments;
    private User user;
    private ExceptionAuthorizedValidator validator;

    @Before
    public void setUpUserAndValidator() {
        validationCallArguments = new ArrayList<>();
        user = new User("normal", Collections.emptySet(), Collections.emptySet()) {

            @Override
            public boolean hasAnyPrivilege(Privilege.Clazz clazz, String ident) {
                validationCallArguments.add(Lists.newArrayList(clazz, ident, user.name()));
                return true;
            }
        };
        validator = new ExceptionPrivilegeValidator(user);
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
        validator.ensureExceptionAuthorized(new RelationValidationException(Lists.newArrayList(
            TableIdent.fromIndexName("users"),
            TableIdent.fromIndexName("my_schema.foo")
        ), "bla"));
        assertAskedAnyForTable("doc.users");
        assertAskedAnyForTable("my_schema.foo");
    }

    @Test
    public void testSchemaScopeException() throws Exception {
        validator.ensureExceptionAuthorized(new SchemaUnknownException("my_schema"));
        assertAskedAnyForSchema("my_schema");
    }

    @Test
    public void testClusterScopeException() throws Exception {
        validator.ensureExceptionAuthorized(new UnsupportedFeatureException("unsupported"));
        assertAskedAnyForCluster();
    }

    @Test
    public void testUnscopedException() throws Exception {
        validator.ensureExceptionAuthorized(new UnhandledServerException("unhandled"));
        assertThat(validationCallArguments.size(), is(0));
    }
}
