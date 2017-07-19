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
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Collections;

public class PrivilegesTest extends CrateUnitTest {

    @Test
    public void testExceptionIsThrownIfUserHasNotRequiredPrivilege() throws Exception {
        User user = new User("ford", Collections.emptySet(), Collections.emptySet());

        expectedException.expect(MissingPrivilegeException.class);
        expectedException.expectMessage("Missing 'DQL' privilege for user 'ford'");
        Privileges.ensureUserHasPrivilege(Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, user);
    }

    @Test
    public void testNoExceptionIsThrownIfUserHasNotRequiredPrivilegeOnInformationSchema() throws Exception {
        User user = new User("ford", Collections.emptySet(), Collections.emptySet());
        //ensureUserHasPrivilege will not throw an exception if the schema is `information_schema`
        Privileges.ensureUserHasPrivilege(Privilege.Type.DQL, Privilege.Clazz.SCHEMA, "information_schema", user);
    }

    @Test
    public void testExceptionIsThrownIfUserHasNotAnyPrivilege() throws Exception {
        User user = new User("ford", Collections.emptySet(), Collections.emptySet());

        expectedException.expect(MissingPrivilegeException.class);
        expectedException.expectMessage("Missing privilege for user 'ford'");
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.CLUSTER, null, user);
    }

    @Test
    public void testUserWithNoPrivilegeCanAccessInformationSchema() throws Exception {
        User user = new User("ford", Collections.emptySet(), Collections.emptySet());

        //ensureUserHasPrivilege will not throw an exception if the schema is `information_schema`
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.SCHEMA, "information_schema", user);
        Privileges.ensureUserHasPrivilege(Privilege.Clazz.TABLE, "information_schema.table", user);
    }
}
