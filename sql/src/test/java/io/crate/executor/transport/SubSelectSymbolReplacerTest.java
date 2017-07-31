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

package io.crate.executor.transport;

import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.node.dql.ESGet;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;

public class SubSelectSymbolReplacerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testSelectSymbolsAreReplacedInSelectListOfPrimaryKeyLookups() throws Exception {
        MultiPhasePlan plan = e.plan("select (select 'foo' from sys.cluster) from users where id = 10");
        ESGet esGet = (ESGet) plan.rootPlan();
        Map<Plan, SelectSymbol> dependencies = plan.dependencies();
        SelectSymbol selectSymbol = dependencies.values().iterator().next();

        SubSelectSymbolReplacer replacer = new SubSelectSymbolReplacer(esGet, selectSymbol);
        replacer.onSuccess(new BytesRef[] {new BytesRef("foo")});

        Symbol output = esGet.outputs().get(0);
        assertThat(output.toString(), is("single_value(['foo'])"));
    }
}
