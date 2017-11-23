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

package io.crate.operation.reference.sys.node.local;

import io.crate.metadata.ReferenceImplementation;
import io.crate.monitor.ExtendedOsStats;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.monitor.os.OsStats;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class NodeOsExpressionTest extends CrateUnitTest {

    @Test
    public void testOsTimestampNotEvaluatedTwice() throws Exception {
        ExtendedOsStats.Cpu cpu = mock(ExtendedOsStats.Cpu.class);
        OsStats osStats = mock(OsStats.class);
        ExtendedOsStats extendedOsStats = new ExtendedOsStats(1000L, cpu, new double[]{-1.0d, -1.0d, -1.0d}, 1000L, osStats, null);
        NodeOsExpression nodeOsExpression = new NodeOsExpression(extendedOsStats);
        ReferenceImplementation timestampExpr = nodeOsExpression.getChildImplementation("timestamp");
        Long ts1 = (Long) timestampExpr.value();
        Thread.sleep(10L);
        Long ts2 = (Long) timestampExpr.value();
        assertEquals(ts1, ts2);
    }
}
