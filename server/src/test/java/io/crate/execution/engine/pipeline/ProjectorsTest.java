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

package io.crate.execution.engine.pipeline;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.engine.aggregation.GroupingProjector;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ProjectorsTest extends CrateDummyClusterServiceUnitTest {

    private ProjectionToProjectorVisitor projectorFactory;
    private OnHeapMemoryManager memoryManager;
    private NodeContext nodeCtx;

    @Before
    public void prepare() throws Exception {
        nodeCtx = createNodeContext();
        memoryManager = new OnHeapMemoryManager(bytes -> {});
        projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            null,
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            new NoneCircuitBreakerService(),
            nodeCtx,
            THREAD_POOL,
            Settings.EMPTY,
            mock(ElasticsearchClient.class),
            new InputFactory(nodeCtx),
            new EvaluatingNormalizer(
                nodeCtx,
                RowGranularity.SHARD,
                r -> Literal.ofUnchecked(r.valueType(), r.valueType().sanitizeValue("1")),
                null),
            t -> null,
            t -> null,
            Version.CURRENT,
            new ShardId("dummy", UUID.randomUUID().toString(), 0),
            null
        );
    }

    @Test
    public void testProjectionsWithCorrectGranularityAreApplied() {
        GroupProjection groupProjection = new GroupProjection(
            new ArrayList<>(), new ArrayList<>(), AggregateMode.ITER_FINAL, RowGranularity.SHARD);
        FilterProjection filterProjection = new FilterProjection(new InputColumn(0), Collections.emptyList());
        filterProjection.requiredGranularity(RowGranularity.DOC);

        Projectors projectors = new Projectors(
            Arrays.asList(filterProjection, groupProjection),
            UUID.randomUUID(),
            CoordinatorTxnCtx.systemTransactionContext(),
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            projectorFactory
        );

        assertThat(projectors.providesIndependentScroll()).isTrue();
        assertThat(projectors.projectors.size(), is(1));
        assertThat(projectors.projectors.get(0), instanceOf(GroupingProjector.class));
    }

}
