/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.action.job;

import io.crate.jobs.CountContext;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.CountNode;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;

@Singleton
public class ExecutionNodeOperationStarter implements RowUpstream {

    private static final String COLLECT_EXECUTOR = ThreadPool.Names.SEARCH;

    private final ThreadPool threadPool;
    private final StatsTables statsTables;
    private final MapSideDataCollectOperation mapSideDataCollectOperation;
    private final InnerStarter innerStarter;

    @Inject
    public ExecutionNodeOperationStarter(ThreadPool threadPool,
                                         StatsTables statsTables,
                                         MapSideDataCollectOperation mapSideDataCollectOperation) {
        this.threadPool = threadPool;
        this.statsTables = statsTables;
        this.mapSideDataCollectOperation = mapSideDataCollectOperation;
        this.innerStarter = new InnerStarter();
    }

    public void startOperation(ExecutionNode executionNode, JobExecutionContext jobExecutionContext) {
        innerStarter.process(executionNode, jobExecutionContext);
    }

    public void startOperations(Collection<? extends ExecutionNode>executionNodes, JobExecutionContext jobExecutionContext) {
        for (ExecutionNode executionNode : executionNodes) {
            startOperation(executionNode, jobExecutionContext);
        }
    }

    private class InnerStarter extends ExecutionNodeVisitor<JobExecutionContext, Void> {

        @Override
        protected Void visitExecutionNode(ExecutionNode node, JobExecutionContext context) {
            throw new UnsupportedOperationException("Can't handle " + node);
        }

        @Override
        public Void visitMergeNode(MergeNode node, JobExecutionContext context) {
            // nothing to do; merge is done by creating a context and then rows/pages are pushed into that context
            return null;
        }

        @Override
        public Void visitCountNode(CountNode countNode, JobExecutionContext context) {
            CountContext countContext = context.getSubContext(countNode.executionNodeId());
            countContext.start();
            return null;
        }

        @Override
        public Void visitCollectNode(final CollectNode collectNode, final JobExecutionContext context) {
            final JobCollectContext collectContext = context.getSubContext(collectNode.executionNodeId());
            threadPool.executor(COLLECT_EXECUTOR).execute(new Runnable() {
                @Override
                public void run() {
                    statsTables.operationStarted(collectNode.executionNodeId(), context.jobId(), collectNode.name());

                    try {
                        mapSideDataCollectOperation.collect(
                                collectNode,
                                collectContext.rowDownstream(), collectContext.ramAccountingContext());
                    } catch (Throwable t) {
                        RowDownstreamHandle rowDownstreamHandle =
                                collectContext.rowDownstream().registerUpstream(ExecutionNodeOperationStarter.this);
                        rowDownstreamHandle.fail(t);
                    }
                }
            });
            return null;
        }
    }
}
