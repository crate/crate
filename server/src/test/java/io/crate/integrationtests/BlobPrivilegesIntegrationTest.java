/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.net.URI;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;

// Because of https://github.com/crate/crate/issues/19335 if more nodes exist in the cluster
// a possible redirect will remove the basic auth headers and make the test flaky, as it leads
// to using default "crate" user when no user is extracted from the headers
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.SUITE, numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
@WindowsIncompatible
public class BlobPrivilegesIntegrationTest extends BlobHttpIntegrationTest {

    @Test
    public void test_missing_privileges() throws Exception {
        Role john = RolesHelper.userOf("john");
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        URI uri = blobUri("test", digest);
        String body = "a".repeat(1500);

        var response = get(uri, john);
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).isEqualTo("Schema 'blob' unknown");

        var response2 = head(uri, john);
        assertThat(response2.statusCode()).isEqualTo(404);

        var response3 = put(uri, body, john);
        assertThat(response3.statusCode()).isEqualTo(404);
        assertThat(response3.body()).isEqualTo("Schema 'blob' unknown");

        var response4 = delete(uri, john);
        assertThat(response4.statusCode()).isEqualTo(404);
    }
}

