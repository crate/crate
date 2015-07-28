/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.ImmutableSet;
import io.crate.executor.transport.*;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingProjector;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class FetchProjectorTest extends CrateUnitTest {

    @Test
    public void testCloseContextsAndFinishOnFail() throws Exception {
        /**
         * Test that the method closeContextsAndFinish() does not raise JobExecutionContextMissingException
         * when node is missing from cluster state
         */
        TransportCloseContextNodeAction transportCloseContextNodeAction = mock(TransportCloseContextNodeAction.class);
        doThrow(new IllegalArgumentException("Node \"missing-node\" not found in cluster state!"))
                .when(transportCloseContextNodeAction).execute(anyString(), any(NodeCloseContextRequest.class), any(ActionListener.class));
        CollectingProjector downstream = new CollectingProjector();

        FetchProjector projector = new FetchProjector(
                mock(TransportFetchNodeAction.class),
                transportCloseContextNodeAction,
                mock(Functions.class),
                UUID.randomUUID(),
                1,
                mock(CollectExpression.class),
                Collections.<Symbol>emptyList(),
                Collections.<Symbol>emptyList(),
                Collections.<ReferenceInfo>emptyList(),
                IntObjectOpenHashMap.<String>newInstance(),
                IntObjectOpenHashMap.<ShardId>newInstance(),
                ImmutableSet.of("missing-node"),
                true);
        projector.registerUpstream(mock(RowUpstream.class));
        projector.downstream(downstream);
        projector.fail(new Throwable("Something when wrong"));

    }
}