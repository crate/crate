/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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


import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.StringStartsWith.startsWith;

public class RestSQLActionIntegrationTest extends SQLHttpIntegrationTest {


    @Test
    public void testWithoutBody() throws IOException {
        CloseableHttpResponse response = post();
        assertEquals(400, response.getStatusLine().getStatusCode());
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith(
                "{\"error\":{\"message\":\"SQLActionException[missing request body]\",\"code\":4000},\"error_trace\":\"SQLActionException:"
        ));
    }

    @Test
    public void testWithInvalidPayload() throws IOException {
        CloseableHttpResponse response = post("{\"foo\": \"bar\"}");
        assertEquals(400, response.getStatusLine().getStatusCode());
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith(
                "{\"error\":{\"message\":\"SQLActionException[Failed to parse source [{\\\"foo\\\": \\\"bar\\\"}]]\",\"code\":4000},\"error_trace\":\""));
    }

    @Test
    public void testWithArgsAndBulkArgs() throws IOException {
        CloseableHttpResponse response = post("{\"stmt\": \"INSERT INTO foo (bar) values (?)\", \"args\": [0], \"bulk_args\": [[0], [1]]}");
        assertEquals(400, response.getStatusLine().getStatusCode());
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, startsWith(
                "{\"error\":{\"message\":\"SQLActionException[request body contains args and bulk_args. It's forbidden to provide both]\",\"code\":4000},\"error_trace\":\"SQLActionException:"
        ));
    }

    private String resolveEscapedNL(){
        String LN = System.getProperty("line.separator");
        // http://en.wikipedia.org/wiki/Newline#Representations
        switch (LN){
            case "\r": return "\\r";
            case "\r\n": return "\\r\\n";
            default: return "\\n";
        }
    }
}
