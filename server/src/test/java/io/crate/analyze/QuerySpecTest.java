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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class QuerySpecTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION);
    }

    @Test
    public void testVisitSymbolVisitsAllSymbols() throws Exception {
        AnalyzedRelation relation = e.analyze(
            "select " +
            "       x," +            // 1
            "       count(*) " +     // 2
            "from t1 " +
            "where x = 2 " +         // 3 (function counts as 1 symbol)
            "group by 1 " +          // 4
            "having count(*) = 2 " + // 5
            "order by 2 " +          // 6
            "limit 1 " +             // 7
            "offset 1");             // 8
        final AtomicInteger numSymbols = new AtomicInteger(0);
        relation.visitSymbols(s -> numSymbols.incrementAndGet());
        assertThat(numSymbols.get()).isEqualTo(8);
    }
}
