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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.Functions;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;

@Singleton
public class SingleRowSource implements CollectSource {

    private final Functions functions;
    private final NodeSysExpression nodeSysExpression;

    @Inject
    public SingleRowSource(Functions functions, NodeSysExpression nodeSysExpression) {
        this.functions = functions;
        this.nodeSysExpression = nodeSysExpression;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        ImplementationSymbolVisitor nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                new NodeSysReferenceResolver(nodeSysExpression),
                functions,
                RowGranularity.NODE
        );
        if (collectPhase.whereClause().noMatch()){
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }
        ImplementationSymbolVisitor.Context ctx = nodeImplementationSymbolVisitor.extractImplementations(collectPhase);
        return ImmutableList.<CrateCollector>of(RowsCollector.single(ctx.topLevelInputs(), downstream));
    }
}
