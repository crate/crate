/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.protocols.postgres.PGErrorStatus;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(numClientNodes = 0)
public class SysNodesITest extends SQLTransportIntegrationTest {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Test
    public void testNoMatchingNode() throws Exception {
        execute("select id, name, hostname from sys.nodes where id = 'does-not-exist'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testScalarEvaluatesInErrorOnSysNodes() throws Exception {
        assertThrows(() -> execute("select 1/0 from sys.nodes"),
                     isSQLError(CoreMatchers.is("/ by zero"),
                                PGErrorStatus.INTERNAL_ERROR,
                                HttpResponseStatus.BAD_REQUEST,
                                4000));

    }

    @Test
    public void testRestUrl() throws Exception {
        execute("select rest_url from sys.nodes");
        assertThat((String) response.rows()[0][0], startsWith("127.0.0.1:"));
    }

}
