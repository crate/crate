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

package io.crate.execution.jobs;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.PKAndVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class PKLookupTask extends AbstractTask {

    private final UUID jobId;
    private final RamAccountingContext ramAccountingContext;
    private final TransactionContext txnCtx;
    private final PKLookupOperation pkLookupOperation;
    private final boolean ignoreMissing;
    private final Map<ShardId, List<PKAndVersion>> idsByShard;
    private final Collection<? extends Projection> shardProjections;
    private final RowConsumer consumer;
    private final InputRow inputRow;
    private final List<CollectExpression<Doc, ?>> expressions;
    private final String name;

    PKLookupTask(UUID jobId,
                 int phaseId,
                 String name,
                 RamAccountingContext ramAccountingContext,
                 TransactionContext txnCtx,
                 InputFactory inputFactory,
                 PKLookupOperation pkLookupOperation,
                 List<ColumnIdent> partitionedByColumns,
                 List<Symbol> toCollect,
                 Map<ShardId, List<PKAndVersion>> idsByShard,
                 Collection<? extends Projection> shardProjections,
                 RowConsumer consumer) {
        super(phaseId);
        this.jobId = jobId;
        this.name = name;
        this.ramAccountingContext = ramAccountingContext;
        this.txnCtx = txnCtx;
        this.pkLookupOperation = pkLookupOperation;
        this.idsByShard = idsByShard;
        this.shardProjections = shardProjections;
        this.consumer = consumer;

        this.ignoreMissing = !partitionedByColumns.isEmpty();
        DocRefResolver docRefResolver = new DocRefResolver(partitionedByColumns);

        InputFactory.Context<CollectExpression<Doc, ?>> ctx = inputFactory.ctxForRefs(txnCtx, docRefResolver);
        ctx.add(toCollect);
        expressions = ctx.expressions();
        inputRow = new InputRow(ctx.topLevelInputs());
    }

    @Override
    protected void innerStart() {
        if (shardProjections.isEmpty()) {
            BatchIterator<Doc> batchIterator = pkLookupOperation.lookup(ignoreMissing, idsByShard, consumer.requiresScroll());
            consumer.accept(BatchIterators.map(batchIterator, this::resultToRow), null);
        } else {
            pkLookupOperation.runWithShardProjections(
                jobId,
                txnCtx,
                ramAccountingContext,
                ignoreMissing,
                idsByShard,
                shardProjections,
                consumer,
                this::resultToRow
            );
        }
        close();
    }

    private Row resultToRow(Doc getResult) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(getResult);
        }
        return inputRow;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long bytesUsed() {
        return ramAccountingContext.totalBytes();
    }
}
