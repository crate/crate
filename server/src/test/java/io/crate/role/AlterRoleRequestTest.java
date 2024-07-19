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

import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class AlterRoleRequestTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        var arr1 =
            new AlterRoleRequest("testUser",
                SecureHash.of(new SecureString("passwd".toCharArray())),
                new JwtProperties("https:dummy.org", "test", "test_aud"),
                true,
                true,
                Map.of(true, Map.of("enable_hashjoin", "false", "statement_timeout", "10m"))
            );

        BytesStreamOutput out = new BytesStreamOutput();
        arr1.writeTo(out);
        var arr2 = new AlterRoleRequest(out.bytes().streamInput());
        assertThat(arr2.roleName()).isEqualTo(arr1.roleName());
        assertThat(arr2.secureHash()).isEqualTo(arr1.secureHash());
        var jwtProps = arr2.jwtProperties();
        assertThat(jwtProps).isNotNull();
        assertThat(jwtProps.iss()).isEqualTo("https:dummy.org");
        assertThat(jwtProps.username()).isEqualTo("test");
        assertThat(jwtProps.aud()).isEqualTo("test_aud");
        assertThat(arr2.resetPassword()).isTrue();
        assertThat(arr2.resetJwtProperties()).isTrue();
        assertThat(arr2.sessionSettingsChange()).isEqualTo(arr1.sessionSettingsChange());

        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_5_0);
        arr1.writeTo(out);
        var in = out.bytes().streamInput();
        in.setVersion(Version.V_5_5_0);
        arr2 = new AlterRoleRequest(in);
        assertThat(arr2.roleName()).isEqualTo(arr1.roleName());
        assertThat(arr2.secureHash()).isEqualTo(arr1.secureHash());
        assertThat(arr2.resetPassword()).isFalse();
        assertThat(arr2.resetJwtProperties()).isFalse();
        assertThat(arr2.jwtProperties()).isNull();
        assertThat(arr2.sessionSettingsChange()).isEmpty();

        out = new BytesStreamOutput();
        out.setVersion(Version.V_5_8_0);
        arr1.writeTo(out);
        in = out.bytes().streamInput();
        in.setVersion(Version.V_5_8_0);
        arr2 = new AlterRoleRequest(in);
        assertThat(arr2.roleName()).isEqualTo(arr1.roleName());
        assertThat(arr2.secureHash()).isEqualTo(arr1.secureHash());
        assertThat(arr2.resetPassword()).isTrue();
        assertThat(arr2.resetJwtProperties()).isTrue();
        assertThat(arr2.jwtProperties()).isEqualTo(arr1.jwtProperties());
        assertThat(arr2.sessionSettingsChange()).isEmpty();
    }

    @Test
    public void testStreaming_with_empty_and_nulls() throws Exception {
        var arr1 =
            new AlterRoleRequest("testUser",
                null,
                null,
                false,
                false,
                Map.of()
            );

        BytesStreamOutput out = new BytesStreamOutput();
        arr1.writeTo(out);
        var arr2 = new AlterRoleRequest(out.bytes().streamInput());
        assertThat(arr2.roleName()).isEqualTo("testUser");
        assertThat(arr2.secureHash()).isNull();
        assertThat(arr2.jwtProperties()).isNull();
        assertThat(arr2.resetPassword()).isFalse();
        assertThat(arr2.resetJwtProperties()).isFalse();
        assertThat(arr2.sessionSettingsChange()).isEmpty();

        arr1 =
            new AlterRoleRequest("testUser",
                null,
                null,
                false,
                false,
                Map.of(true, Map.of())
            );

        out = new BytesStreamOutput();
        arr1.writeTo(out);
        arr2 = new AlterRoleRequest(out.bytes().streamInput());
        assertThat(arr2.roleName()).isEqualTo("testUser");
        assertThat(arr2.secureHash()).isNull();
        assertThat(arr2.jwtProperties()).isNull();
        assertThat(arr2.resetPassword()).isFalse();
        assertThat(arr2.resetJwtProperties()).isFalse();
        assertThat(arr2.sessionSettingsChange()).containsExactly(Map.entry(true, Map.of()));
    }
}
