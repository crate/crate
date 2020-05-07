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
import io.crate.breaker.CrateCircuitBreakerService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class CircuitBreakerIntegrationTest extends SQLTransportIntegrationTest {

    @After
    public void resetBreakerLimit() {
        execute("reset global \"indices.breaker.query.limit\"");
    }

    @Test
    public void testQueryBreakerIsDecrementedWhenQueryCompletes() {
        execute("create table t1 (text string)");
        ensureYellow();
        execute("insert into t1 values ('this is some text'), ('other text')");
        refresh();

        CrateCircuitBreakerService circuitBreakerService = internalCluster().getInstance(CrateCircuitBreakerService.class);
        CircuitBreaker queryBreaker = circuitBreakerService.getBreaker(CrateCircuitBreakerService.QUERY);
        long breakerBytesUsedBeforeQuery = queryBreaker.getUsed();

        execute("select text from t1 group by text");

        assertThat(queryBreaker.getUsed(), is(breakerBytesUsedBeforeQuery));
    }

    @Test
    public void testQueryBreakerIsUpdatedWhenSettingIsChanged() {
        execute("create table t1 (text string) clustered into 1 shards");
        ensureYellow();
        execute("insert into t1 values ('this is some text'), ('other text')");
        refresh();

        execute("select text from t1 group by text");

        execute("set global \"indices.breaker.query.limit\"='100b'");
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("CircuitBreakingException: [query] Data too large, data for [collect: 0] " +
                                        "would be [130/130b], which is larger than the limit of [100/100b]");
        execute("select text from t1 group by text");
    }
}
