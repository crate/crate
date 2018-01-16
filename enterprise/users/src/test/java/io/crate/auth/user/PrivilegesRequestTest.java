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

import com.google.common.collect.Lists;
import io.crate.analyze.user.Privilege;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class PrivilegesRequestTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        List<String> users = Lists.newArrayList("ford", "arthur");
        List<Privilege> privileges = Lists.newArrayList(
            new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate")
        );
        PrivilegesRequest r1 = new PrivilegesRequest(users, privileges);

        BytesStreamOutput out = new BytesStreamOutput();
        r1.writeTo(out);

        PrivilegesRequest r2 = new PrivilegesRequest();
        r2.readFrom(out.bytes().streamInput());

        assertThat(r2.userNames(), is(users));
        assertThat(r2.privileges(), is(privileges));
    }
}
