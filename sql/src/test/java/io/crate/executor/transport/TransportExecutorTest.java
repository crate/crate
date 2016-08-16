/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.StmtCtx;
import io.crate.metadata.TableIdent;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.management.KillPlan;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.isRow;
import static java.util.Arrays.asList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;

public class TransportExecutorTest extends BaseTransportExecutorTest {

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = newPlannerContext();
        Plan plan = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(2, "Ford")));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC));
        Planner.Context ctx = newPlannerContext();
        Plan plan = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(2, null)));
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = newPlannerContext();
        Plan plan = newGetNode("characters", outputs, asList("1", "2"), ctx.nextExecutionPhaseId());
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        assertThat(rowReceiver.result().size(), is(2));
    }

    @Test
    public void testKillTask() throws Exception {
        CollectingRowReceiver downstream = new CollectingRowReceiver();
        KillPlan plan = new KillPlan(UUID.randomUUID());
        executor.execute(plan, downstream);
        downstream.resultFuture().get(5, TimeUnit.SECONDS);
    }

    protected Planner.Context newPlannerContext() {
        return new Planner.Context(clusterService(), UUID.randomUUID(), null, new StmtCtx(), 0, 0);
    }
}
