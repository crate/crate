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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.shard.unassigned.UnassignedShardsReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.common.inject.Inject;

import java.util.Collection;

public class UnassignedShardsCollectSource {

    private final CollectInputSymbolVisitor<Input<?>> inputSymbolVisitor;

    @Inject
    @SuppressWarnings("unchecked")
    public UnassignedShardsCollectSource(Functions functions,
                                         UnassignedShardsReferenceResolver unassignedShardsReferenceResolver) {
        this.inputSymbolVisitor = new CollectInputSymbolVisitor(
                functions,
                unassignedShardsReferenceResolver
        );
    }

    @SuppressWarnings("unchecked")
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase,
                                                    Iterable<? extends UnassignedShard> unassignedShards,
                                                    RowReceiver downstream) {
        if (collectPhase.whereClause().noMatch()){
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }

        CollectInputSymbolVisitor.Context context = inputSymbolVisitor.extractImplementations(collectPhase);
        Input<Boolean> condition;
        if (collectPhase.whereClause().hasQuery()) {
            condition = (Input<Boolean>) inputSymbolVisitor.process(collectPhase.whereClause().query(), context);
        } else {
            condition = Literal.newLiteral(true);
        }
        return ImmutableList.<CrateCollector>of(new RowsCollector<>(
                context.topLevelInputs(), context.docLevelExpressions(), downstream, unassignedShards, condition));
    }
}
