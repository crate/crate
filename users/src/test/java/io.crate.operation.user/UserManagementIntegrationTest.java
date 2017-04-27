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

import io.crate.action.sql.SQLActionException;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UserManagementIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testCreateUser() throws Exception {
        execute("create user fridolin");
        assertThat(response.rowCount(), is(1L));
        // it's not possible to create the same user again
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(containsString("User already exists"));
        execute("create user fridolin");
    }
}
