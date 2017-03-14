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

import com.google.common.collect.Iterables;
import io.crate.action.job.SharedShardContexts;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.ParameterContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.Routing;
import io.crate.metadata.TransactionContext;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.QueryAndFetchConsumer;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.sql.parser.SqlParser;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Create preconfigured CrateCollectors from a SQL statement and a TestCluster.
 * Returns all collectors from all TestCluster nodes.
 */
public class LuceneDocCollectorProvider implements AutoCloseable {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    private final InternalTestCluster cluster;
    private final Analyzer analyzer;
    private final QueryAndFetchConsumer queryAndFetchConsumer;
    private final EvaluatingNormalizer normalizer;
    private final Planner planner;

    private List<JobCollectContext> collectContexts = new ArrayList<>();

    public LuceneDocCollectorProvider(InternalTestCluster cluster) {
        this.cluster = cluster;
        this.analyzer = cluster.getDataNodeInstance(Analyzer.class);
        this.planner = cluster.getDataNodeInstance(Planner.class);
        this.queryAndFetchConsumer = cluster.getDataNodeInstance(QueryAndFetchConsumer.class);
        this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(
            cluster.getInstance(Functions.class), ReplaceMode.COPY);
    }

    private Iterable<CrateCollector> createNodeCollectors(String nodeId, RoutedCollectPhase collectPhase, BatchConsumer downstream) throws Exception {
        String nodeName = cluster.clusterService().state().nodes().get(nodeId).name();
        IndicesService indicesService = cluster.getInstance(IndicesService.class, nodeName);
        JobContextService jobContextService = cluster.getInstance(JobContextService.class, nodeName);
        MapSideDataCollectOperation collectOperation = cluster.getInstance(MapSideDataCollectOperation.class, nodeName);

        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService);
        JobExecutionContext.Builder builder = jobContextService.newBuilder(collectPhase.jobId());
        JobCollectContext jobCollectContext = new JobCollectContext(
            collectPhase, collectOperation, cluster.clusterService().state().nodes().getLocalNodeId(),
            RAM_ACCOUNTING_CONTEXT, downstream, sharedShardContexts);
        collectContexts.add(jobCollectContext);
        builder.addSubContext(jobCollectContext);
        jobContextService.createContext(builder);
        return jobCollectContext.collectors();
    }

    public CrateCollector createCollector(String statement, final BatchConsumer downstream, Integer nodePageSizeHint, Object... args) throws Exception {
        Analysis analysis = analyzer.boundAnalyze(
            SqlParser.createStatement(statement),
            SessionContext.SYSTEM_SESSION,
            new ParameterContext(new RowN(args), Collections.<Row>emptyList()));
        Plan plan = queryAndFetchConsumer.consume(
            analysis.rootRelation(),
            new ConsumerContext(new Planner.Context(planner,
                cluster.clusterService(), UUID.randomUUID(), null, normalizer, new TransactionContext(SessionContext.SYSTEM_SESSION), 0, 0)));
        Collect collect = (Collect) plan;
        final RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        collectPhase.nodePageSizeHint(nodePageSizeHint);
        Routing routing = collectPhase.routing();
        String nodeId = Iterables.getOnlyElement(routing.nodes());
        return Iterables.getOnlyElement(createNodeCollectors(nodeId, collectPhase, downstream));
    }

    @Override
    public void close() throws Exception {
        for (JobCollectContext ctx : collectContexts) {
            ctx.close();
        }
        collectContexts.clear();
    }
}
