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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class FlatProjectorChainTest extends CrateUnitTest {

    @Test
    public void testFinalDownstreamPrepareIsCalled() throws Exception {
        RowReceiver finalDownstream = mock(RowReceiver.class);
        TopNProjection topN = new TopNProjection(0, 1);
        ProjectorFactory factory = mock(ProjectorFactory.class);
        RamAccountingContext ramAccountingContext = mock(RamAccountingContext.class);

        UUID jobId = UUID.randomUUID();
        when(factory.create(topN, ramAccountingContext, jobId)).thenReturn(new SimpleTopNProjector(ImmutableList.<Input<?>>of(), Collections.<CollectExpression<Row, Object>>emptyList(), 0, 1));
        FlatProjectorChain chain = FlatProjectorChain.withAttachedDownstream(factory, ramAccountingContext, ImmutableList.<Projection>of(topN), finalDownstream, jobId);
        chain.prepare();

        verify(finalDownstream, times(1)).prepare();

    }
}
