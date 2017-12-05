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
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.executor.transport.task.elasticsearch.GetResponseRefResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.operation.InputFactory;
import io.crate.operation.InputRow;
import io.crate.operation.PKLookupOperation;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.operators.PKAndVersion;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Map;

public final class PKLookupContext extends AbstractExecutionSubContext {

    private static final Logger LOGGER = Loggers.getLogger(PKLookupContext.class);
    private final PKLookupOperation pkLookupOperation;
    private final boolean ignoreMissing;
    private final Map<ShardId, List<PKAndVersion>> idsByShard;
    private final RowConsumer consumer;
    private final InputRow inputRow;
    private final List<CollectExpression<GetResponse, ?>> expressions;

    public PKLookupContext(int phaseId,
                           InputFactory inputFactory,
                           PKLookupOperation pkLookupOperation,
                           List<ColumnIdent> partitionedByColumns,
                           List<Symbol> toCollect,
                           Map<ShardId, List<PKAndVersion>> idsByShard,
                           RowConsumer consumer) {
        super(phaseId, LOGGER);
        this.pkLookupOperation = pkLookupOperation;
        this.idsByShard = idsByShard;
        this.consumer = consumer;
        this.ignoreMissing = !partitionedByColumns.isEmpty();
        GetResponseRefResolver getResponseRefResolver = new GetResponseRefResolver(partitionedByColumns);

        InputFactory.Context<CollectExpression<GetResponse, ?>> ctx = inputFactory.ctxForRefs(getResponseRefResolver);
        ctx.add(toCollect);
        expressions = ctx.expressions();
        inputRow = new InputRow(ctx.topLevelInputs());
    }

    @Override
    protected void innerStart() {
        BatchIterator<GetResult> batchIterator = pkLookupOperation.lookup(ignoreMissing, idsByShard);
        consumer.accept(BatchIterators.map(batchIterator, this::resultToRow), null);
        close(null);
    }

    private Row resultToRow(GetResult getResult) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(new GetResponse(getResult));
        }
        return inputRow;
    }

    @Override
    public String name() {
        return "pkLookup";
    }
}
