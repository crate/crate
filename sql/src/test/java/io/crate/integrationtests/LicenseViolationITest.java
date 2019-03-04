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

import io.crate.action.sql.SQLActionException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class LicenseViolationITest extends SQLTransportIntegrationTest {

    @Test
    public void testThatOperationIsAllowedWhenNodesAreWithinMaxNodesThreshold() throws Exception {
        execute("create table t1 (i int)");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testThatOperationIsNotAllowedWhenNodesExceedMaxNodesThreshold() throws Exception {
        // start another node so the number of nodes is exceeds the license threshold
        internalCluster().startNodes(1);

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("License is violated");
        execute("create table t1 (i int)");
    }

    @Test
    public void testThatOperationIsAllowedWhenNodesAreBackWithinMaxNodesThreshold() throws Exception {
        // stop any node so the number of nodes is within the license threshold
        internalCluster().stopRandomDataNode();

        execute("create table t1 (i int)");
        assertThat(response.rowCount(), is(1L));
    }
}
