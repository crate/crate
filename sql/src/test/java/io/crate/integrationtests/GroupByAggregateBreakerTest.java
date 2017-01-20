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

import io.crate.action.sql.SQLActionException;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

@UseJdbc
public class GroupByAggregateBreakerTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        RamAccountingContext.FLUSH_BUFFER_SIZE = 24;
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CrateCircuitBreakerService.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "256b")
            .build();
    }

    @Before
    public void initTestData() {
        Setup setup = new Setup(sqlExecutor);
        setup.setUpEmployees();
    }

    @Test
    public void selectGroupByWithBreaking() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("CircuitBreakingException: [query] Data too large, data for ");
        // query takes 252 bytes of memory
        // 252b * 1.09 = 275b => should break with limit 256b
        execute("select name, department, max(income), min(age) from employees group by name, department order by 3");
    }
}
