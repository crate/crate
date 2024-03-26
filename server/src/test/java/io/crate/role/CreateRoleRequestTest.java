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

package io.crate.role;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class CreateRoleRequestTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        var crr1 =
            new CreateRoleRequest("testUser",
                false,
                SecureHash.of(new SecureString("passwd".toCharArray())),
                new JwtProperties("https:dummy.org", "test", "test_aud")
            );

        BytesStreamOutput out = new BytesStreamOutput();
        crr1.writeTo(out);
        var crr2 = new CreateRoleRequest(out.bytes().streamInput());
        assertThat(crr2.roleName()).isEqualTo(crr1.roleName());
        assertThat(crr2.secureHash()).isEqualTo(crr1.secureHash());
        assertThat(crr2.isUser()).isFalse();
        var jwtProps = crr2.jwtProperties();
        assertThat(jwtProps).isNotNull();
        assertThat(jwtProps.iss()).isEqualTo("https:dummy.org");
        assertThat(jwtProps.username()).isEqualTo("test");
        assertThat(jwtProps.aud()).isEqualTo("test_aud");

        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_5_0);
        crr1.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_5_5_0);
        crr2 = new CreateRoleRequest(in);
        assertThat(crr2.roleName()).isEqualTo(crr1.roleName());
        assertThat(crr2.secureHash()).isEqualTo(crr1.secureHash());
        assertThat(crr2.isUser()).isTrue();
        assertThat(crr2.jwtProperties()).isNull();
    }
}
