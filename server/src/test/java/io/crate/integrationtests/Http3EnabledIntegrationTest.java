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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.netty.handler.codec.quic.Quic;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class Http3EnabledIntegrationTest extends Http3IntegrationTest {

    @Override
    protected boolean http3Enabled() {
        return true;
    }

    @Test
    public void test_https_only_client_can_run_sql() throws Exception {
        assertSelectSucceeded(postHttpSelect());
    }

    @Test
    public void test_https_only_client_receives_alt_svc_when_quic_is_available() throws Exception {
        assumeThat(Quic.isAvailable())
            .as("native QUIC must be available on this platform")
            .isTrue();

        var response = postHttpSelect();
        assertSelectSucceeded(response);
        assertThat(altSvcFromHeader(response)).hasValue(expectedAltSvcValue());
    }

    @Test
    public void test_falls_back_to_https_when_http3_enabled_but_quic_cannot_start() throws Exception {
        assumeThat(Quic.isAvailable())
            .as("native QUIC must be unavailable to simulate failed HTTP/3 startup")
            .isFalse();

        var response = postHttpSelect();
        assertSelectSucceeded(response);
        assertThat(altSvcFromHeader(response)).isEmpty();
        assertHttp3ClientCannotConnect();
    }

    @Test
    public void test_explicit_http3_client_can_run_sql_when_quic_is_available() throws Exception {
        assumeThat(Quic.isAvailable())
            .as("native QUIC must be available on this platform")
            .isTrue();

        assertThat(postHttp3Select()).contains("\"rowcount\":1");
    }

}
