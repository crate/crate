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

package io.crate.jobs;

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.primarykey.PrimaryKeyLookupOperation;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Context which holds the state for the execution of the primary key lookup operation.
 */
public class PrimaryKeyLookupContext extends AbstractExecutionSubContext {

    private static final Logger LOGGER = Loggers.getLogger(CountContext.class);

    private final PrimaryKeyLookupOperation primaryKeyLookupOperation;
    private final BatchConsumer consumer;
    private final Map<ShardId, List<DocKeys.DocKey>> localDocKeysPerShard;
    private final Map<ColumnIdent, Integer> pkMapping;
    private final List<Symbol> toCollect;
    private CompletableFuture<Iterable<Row>> primaryKeyLookupFuture;

    public PrimaryKeyLookupContext(
            int id,
            PrimaryKeyLookupOperation primaryKeyLookupOperation,
            BatchConsumer consumer,
            Map<ColumnIdent, Integer> pkMapping,
            Map<ShardId, List<DocKeys.DocKey>> localDocKeysPerShard,
            List<Symbol> toCollect) {
        super(id, LOGGER);
        this.primaryKeyLookupOperation = primaryKeyLookupOperation;
        this.consumer = consumer;
        this.localDocKeysPerShard = localDocKeysPerShard;
        this.pkMapping = pkMapping;
        this.toCollect = toCollect;
    }

    @Override
    public synchronized void innerStart() {
        try {
            primaryKeyLookupFuture = primaryKeyLookupOperation.primaryKeyLookup(
                localDocKeysPerShard,
                pkMapping,
                toCollect
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("PrimaryKeyLookUp failed", e);
        }

        primaryKeyLookupFuture.whenComplete((result, ex) -> {
            BatchIterator iterator = null;
            if (ex == null) {
                iterator = RowsBatchIterator.newInstance(result, toCollect.size());
            }
            consumer.accept(iterator, ex);
            close();
        });
    }

    @Override
    public synchronized void innerKill(@Nonnull Throwable throwable) {
        if (primaryKeyLookupFuture == null) {
            consumer.accept(null, throwable);
        } else {
            primaryKeyLookupFuture.cancel(true);
        }
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
    }

    @Override
    public String name() {
        return "primary-key-lookup";
    }
}
