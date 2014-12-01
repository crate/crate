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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.action.sql.SQLActionException;
import io.crate.breaker.CircuitBreakerModule;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Field;
import java.util.Map;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE)
public class GroupByAggregateBreakerTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }


    private Setup setup = new Setup(sqlExecutor);
    private boolean setUpDone = false;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        RamAccountingContext.FLUSH_BUFFER_SIZE = 24;
        return ImmutableSettings.builder()
                .put(CircuitBreakerModule.QUERY_CIRCUIT_BREAKER_LIMIT_SETTING, 512)
                .build();
    }

    @Before
    public void initTestData() {
        if (!setUpDone) {
            this.setup.setUpEmployees();
            setUpDone = true;
        }
    }

    @After
    public void checkStates() throws Exception {
        Field contextManagerField = TransportMergeNodeAction.class.getDeclaredField("contextManager");
        contextManagerField.setAccessible(true);
        Field activeMergeOperationsField = DistributedRequestContextManager.class.getDeclaredField("activeMergeOperations");
        activeMergeOperationsField.setAccessible(true);
        for (String node : new String[]{"node_0", "node_1"}) {
            TransportMergeNodeAction transportMergeNodeAction = cluster().getInstance(TransportMergeNodeAction.class, node);
            DistributedRequestContextManager contextManager = (DistributedRequestContextManager)contextManagerField.get(transportMergeNodeAction);
            assertThat(((Map)activeMergeOperationsField.get(contextManager)).size(), is(0));
        }
    }

    @Repeat(iterations = 10)
    @Test
    public void selectGroupByWithBreaking() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(Matchers.startsWith("Too much HEAP memory used by "));
        execute("select name, department, max(income), min(age) from employees group by name, department order by 3");
    }
}
