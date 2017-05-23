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
import io.crate.planner.Merge;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.PrimaryKeyLookupPhase;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.contains;

public class SubSelectSymbolReplacerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {

        prepareRoutingForIndices(Collections.singletonList("users"));

        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testSelectSymbolsAreReplacedInSelectListOfPrimaryKeyLookups() throws Exception {
        Merge plan = e.plan("select (select 'foo' from sys.cluster) from users where id = 10");
        MultiPhasePlan esGet = (MultiPhasePlan) plan.subPlan();
        Collect plan1 = (Collect) esGet.rootPlan();
        PrimaryKeyLookupPhase lookupPhase = (PrimaryKeyLookupPhase) plan1.collectPhase();

        SelectSymbol subSelect = (SelectSymbol) lookupPhase.toCollect().get(0);

        SubSelectSymbolReplacer replacer = new SubSelectSymbolReplacer(plan1, subSelect);
        replacer.onSuccess(new BytesRef("foo"));

        assertThat(lookupPhase.toCollect(), contains(isLiteral("foo")));
    }
}
