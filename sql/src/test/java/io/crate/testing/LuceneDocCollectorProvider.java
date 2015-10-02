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

package io.crate.testing;

import com.google.common.collect.ImmutableList;
import io.crate.action.job.SharedShardContexts;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Routing;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.Planner;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.QueryAndFetchConsumer;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.sql.parser.SqlParser;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Create preconfigured CrateCollectors from a SQL statement and a TestCluster.
 * Returns all collectors from all TestCluster nodes.
 */
public class LuceneDocCollectorProvider implements AutoCloseable {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    private final InternalTestCluster cluster;
    private final Analyzer analyzer;
    private final QueryAndFetchConsumer queryAndFetchConsumer;

    private List<JobCollectContext> collectContexts = new ArrayList<>();

    public LuceneDocCollectorProvider(InternalTestCluster cluster) {
        this.cluster = cluster;
        this.analyzer = cluster.getDataNodeInstance(Analyzer.class);
        this.queryAndFetchConsumer = cluster.getDataNodeInstance(QueryAndFetchConsumer.class);
    }

    private Iterable<CrateCollector> createNodeCollectors(String nodeId, CollectPhase collectPhase, RowReceiver downstream) {
        String nodeName = cluster.clusterService().state().nodes().get(nodeId).name();
        IndicesService indicesService = cluster.getInstance(IndicesService.class, nodeName);
        JobContextService jobContextService = cluster.getInstance(JobContextService.class, nodeName);
        MapSideDataCollectOperation collectOperation = cluster.getInstance(MapSideDataCollectOperation.class, nodeName);

        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService);
        JobExecutionContext.Builder builder = jobContextService.newBuilder(collectPhase.jobId());
        JobCollectContext jobCollectContext = new JobCollectContext(collectPhase, collectOperation, RAM_ACCOUNTING_CONTEXT, downstream, sharedShardContexts);
        collectContexts.add(jobCollectContext);
        builder.addSubContext(jobCollectContext);
        jobContextService.createContext(builder);

        return collectOperation.createCollectors(
            collectPhase,
            downstream,
            jobCollectContext
        );
    }

    public Iterable<CrateCollector> createCollectors(String statement, final RowReceiver downstream, Object ... args) {
        Analysis analysis = analyzer.analyze(
                SqlParser.createStatement(statement), new ParameterContext(args, new Object[0][], null));
        PlannedAnalyzedRelation plannedAnalyzedRelation = queryAndFetchConsumer.consume(
                analysis.rootRelation(),
                new ConsumerContext(analysis.rootRelation(), new Planner.Context(cluster.clusterService(), UUID.randomUUID(), null)));
        final CollectPhase collectPhase = ((CollectAndMerge) plannedAnalyzedRelation.plan()).collectPhase();

        final ImmutableList.Builder<CrateCollector> builder = ImmutableList.builder();
        Routing routing = collectPhase.routing();
        routing.walkLocations(new Routing.RoutingLocationVisitor() {
            @Override
            public boolean visitNode(String nodeId, Map<String, List<Integer>> nodeRouting) {
                builder.addAll(createNodeCollectors(nodeId, collectPhase, downstream));
                return true;
            }
        });
        return builder.build();
    }

    @Override
    public void close() throws Exception {
        for (JobCollectContext ctx : collectContexts) {
            ctx.close();
        }
        collectContexts.clear();
    }
}
