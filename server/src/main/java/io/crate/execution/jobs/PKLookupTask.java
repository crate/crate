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

package io.crate.execution.jobs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.PKLookupOperation;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.MemoryManager;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.PKAndVersion;

public final class PKLookupTask extends AbstractTask {

    private final UUID jobId;
    private final RamAccounting ramAccounting;
    private final TransactionContext txnCtx;
    private final PKLookupOperation pkLookupOperation;
    private final boolean ignoreMissing;
    private final Map<ShardId, List<PKAndVersion>> idsByShard;
    private final Collection<? extends Projection> shardProjections;
    private final RowConsumer consumer;
    private final InputRow inputRow;
    private final List<CollectExpression<Doc, ?>> expressions;
    private final String name;
    private final Function<RamAccounting, MemoryManager> memoryManagerFactory;
    private final int ramAccountingBlockSizeInBytes;
    private final ArrayList<MemoryManager> memoryManagers = new ArrayList<>();
    private long totalBytes = -1;

    @Nullable
    private final StoredRowLookup storedRowLookup;

    PKLookupTask(UUID jobId,
                 int phaseId,
                 String name,
                 RamAccounting ramAccounting,
                 Function<RamAccounting, MemoryManager> memoryManagerFactory,
                 int ramAccountingBlockSizeInBytes,
                 TransactionContext txnCtx,
                 Schemas schemas,
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
        this.ramAccounting = ramAccounting;
        this.txnCtx = txnCtx;
        this.pkLookupOperation = pkLookupOperation;
        this.idsByShard = idsByShard;
        this.shardProjections = shardProjections;
        this.consumer = consumer;
        this.memoryManagerFactory = memoryManagerFactory;
        this.ramAccountingBlockSizeInBytes = ramAccountingBlockSizeInBytes;

        this.ignoreMissing = !partitionedByColumns.isEmpty();
        DocRefResolver docRefResolver = new DocRefResolver(partitionedByColumns);

        InputFactory.Context<CollectExpression<Doc, ?>> ctx = inputFactory.ctxForRefs(txnCtx, docRefResolver);
        ctx.add(toCollect);

        var shardIt = idsByShard.keySet().iterator();
        if (shardIt.hasNext()) {
            String indexName = shardIt.next().getIndexName();
            var relationName = RelationName.fromIndexName(indexName);
            this.storedRowLookup = StoredRowLookup.create(schemas.getTableInfo(relationName), indexName);
            this.storedRowLookup.register(toCollect);
        } else {
            this.storedRowLookup = null;
        }

        expressions = ctx.expressions();
        inputRow = new InputRow(ctx.topLevelInputs());
    }

    @Override
    protected CompletableFuture<Void> innerStart() {
        BatchIterator<Row> rowBatchIterator = pkLookupOperation.lookup(
            jobId,
            txnCtx,
            this::getRamAccounting,
            this::getMemoryManager,
            ignoreMissing,
            idsByShard,
            shardProjections,
            consumer.requiresScroll(),
            this::resultToRow,
            storedRowLookup
        );
        consumer.accept(rowBatchIterator, null);
        close();
        return null;
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
    protected void innerClose() {
        totalBytes = ramAccounting.totalBytes();
        synchronized (memoryManagers) {
            for (MemoryManager memoryManager : memoryManagers) {
                memoryManager.close();
            }
            memoryManagers.clear();
        }
    }

    @Override
    public long bytesUsed() {
        if (totalBytes == -1) {
            return ramAccounting.totalBytes();
        } else {
            return totalBytes;
        }
    }

    public RamAccounting getRamAccounting() {
        // No tracking/close of BlockBasedRamAccounting
        // to avoid double-release of bytes when the parent instance (`ramAccounting`) is closed.
        return new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes);
    }

    public MemoryManager getMemoryManager() {
        MemoryManager memoryManager = memoryManagerFactory.apply(ramAccounting);
        synchronized (memoryManagers) {
            memoryManagers.add(memoryManager);
        }
        return memoryManager;
    }
}
