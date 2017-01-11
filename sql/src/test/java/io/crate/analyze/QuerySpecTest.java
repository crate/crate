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

package io.crate.analyze;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;

public class QuerySpecTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testVisitSymbolVisitsAllSymbols() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze(
            "select " +
            "       x," +           // 1
            "       count(*) " +    // 2
            "from t1 " +
            "where x = 2 " +        // 3 (function counts as 1 symbol)
            "group by 1 " +         // 4
            "having count(*) = 2 " +// 5
            "order by 2 " +         // 6
            "limit 1 " +            // 7
            "offset 1");            // 8
        final AtomicInteger numSymbols = new AtomicInteger(0);
        stmt.relation().querySpec().visitSymbols(symbol -> numSymbols.incrementAndGet());
        assertThat(numSymbols.get(), is(8));
    }
}
