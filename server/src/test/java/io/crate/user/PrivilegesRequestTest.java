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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class PrivilegesRequestTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        List<String> users = List.of("ford", "arthur");
        List<Privilege> privileges = List.of(
            new Privilege(Privilege.State.GRANT, Privilege.Type.DQL, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.CLUSTER, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.SCHEMA, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DDL, Privilege.Clazz.TABLE, null, "crate"),
            new Privilege(Privilege.State.GRANT, Privilege.Type.DML, Privilege.Clazz.VIEW, null, "crate")
        );
        PrivilegesRequest r1 = new PrivilegesRequest(users, privileges);

        BytesStreamOutput out = new BytesStreamOutput();
        r1.writeTo(out);

        PrivilegesRequest r2 = new PrivilegesRequest(out.bytes().streamInput());

        assertThat(r2.userNames()).isEqualTo(users);
        assertThat(r2.privileges()).isEqualTo(privileges);
    }
}
