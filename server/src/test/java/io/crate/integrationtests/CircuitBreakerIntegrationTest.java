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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class CircuitBreakerIntegrationTest extends IntegTestCase {

    @After
    public void resetBreakerSettings() {
        execute("reset global \"indices.breaker.query.limit\", \"indices.breaker.policy\"");
    }

    @Test
    public void testQueryBreakerIsDecrementedWhenQueryCompletes() throws Exception {
        execute("create table t1 (text string)");
        execute("insert into t1 values ('this is some text'), ('other text')");
        execute("refresh table t1");

        CircuitBreakerService circuitBreakerService = cluster().getInstance(CircuitBreakerService.class);
        CircuitBreaker queryBreaker = circuitBreakerService.getBreaker(CircuitBreaker.QUERY);
        long breakerBytesUsedBeforeQuery = queryBreaker.getUsed();

        execute("select text from t1 group by text");

        assertBusy(() -> {
            assertThat(queryBreaker.getUsed()).isEqualTo(breakerBytesUsedBeforeQuery);
        });
    }

    @Test
    public void testQueryBreakerIsUpdatedWhenSettingIsChanged() {
        execute("create table t1 (text string) clustered into 1 shards");
        execute("insert into t1 values ('this is some text'), ('other text')");
        execute("refresh table t1");

        execute("select text from t1 group by text");

        execute("set global \"indices.breaker.query.limit\"='100b'");

        Asserts.assertSQLError(() -> execute("select text from t1 group by text"))
            .hasMessageContaining("Allocating 120b for 'collect: 0' failed, breaker would use 120b in total. Limit is 100b. Either increase memory and limit, change the query or reduce concurrent query load")
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000);
    }

    @Test
    public void test_top_consumer_policy_invokes_kill() throws Exception {
        execute("set global \"indices.breaker.policy\"='top_consumer'");
        execute("set global \"indices.breaker.query.limit\"='100b'");
        assertThatThrownBy(() -> execute("select distinct x from generate_series(1, 100) as t (x)"))
            .hasMessageContaining("Job killed. Circuit breaker for 'tableFunction: 0' triggered.");
    }
}
