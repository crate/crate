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

package io.crate.operation.projectors;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.operation.AggregationContext;
import io.crate.data.Input;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.crate.operation.projectors.RowReceiver.Result.CONTINUE;
import static io.crate.operation.projectors.RowReceiver.Result.STOP;

public class GroupingProjector extends AbstractProjector {


    private static final ESLogger logger = Loggers.getLogger(GroupingProjector.class);

    private final BiConsumer<Map<Object, Object[]>, Row> accumulator;
    private final Map<Object, Object[]> statesByKey;
    private final Function<Map<Object, Object[]>, Iterable<Row>> finisher;
    private final RamAccountingContext ramAccountingContext;
    private final GroupingCollector<Object> collector;
    private final int numCols;

    private EnumSet<Requirement> requirements;
    private boolean killed = false;
    private IterableRowEmitter rowEmitter;

    public GroupingProjector(List<? extends DataType> keyTypes,
                             List<Input<?>> keyInputs,
                             CollectExpression<Row, ?>[] collectExpressions,
                             AggregationContext[] aggregations,
                             RamAccountingContext ramAccountingContext) {
        this.ramAccountingContext = ramAccountingContext;
        assert keyTypes.size() == keyInputs.size() : "number of key types must match with number of key inputs";
        assert allTypesKnown(keyTypes) : "must have a known type for each key input";

        Aggregator[] aggregators = new Aggregator[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            aggregators[i] = new Aggregator(
                ramAccountingContext,
                aggregations[i].symbol(),
                aggregations[i].function(),
                aggregations[i].inputs()
            );
        }
        if (keyInputs.size() == 1) {
            collector = GroupingCollector.singleKey(
                collectExpressions,
                aggregators,
                ramAccountingContext,
                keyInputs.get(0),
                keyTypes.get(0)
            );
        } else {
            //noinspection unchecked
            collector = (GroupingCollector<Object>) (GroupingCollector) GroupingCollector.manyKeys(
                collectExpressions,
                aggregators,
                ramAccountingContext,
                keyInputs,
                keyTypes
            );
        }
        statesByKey = collector.supplier().get();
        accumulator = collector.accumulator();
        finisher = collector.finisher();
        numCols = keyInputs.size() + aggregators.length;
    }

    private static boolean allTypesKnown(List<? extends DataType> keyTypes) {
        return Iterables.all(keyTypes, new Predicate<DataType>() {
            @Override
            public boolean apply(@Nullable DataType input) {
                return input != null && !input.equals(DataTypes.UNDEFINED);
            }
        });
    }

    @Override
    public Result setNextRow(Row row) {
        if (killed) {
            return STOP;
        }
        accumulator.accept(statesByKey, row);
        return CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        rowEmitter = new IterableRowEmitter(downstream, finisher.apply(statesByKey));
        rowEmitter.run();
        if (logger.isDebugEnabled()) {
            logger.debug("grouping operation size is: {}", new ByteSizeValue(ramAccountingContext.totalBytes()));
        }
    }

    @Override
    public void kill(Throwable throwable) {
        killed = true;
        if (rowEmitter != null) {
            rowEmitter.kill(throwable);
        }
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        if (requirements == null) {
            requirements = Sets.newEnumSet(downstream.requirements(), Requirement.class);
            requirements.remove(Requirement.REPEAT);
        }
        return requirements;
    }

    @Nullable
    @Override
    public java.util.function.Function<BatchIterator, Tuple<BatchIterator, RowReceiver>> batchIteratorProjection() {
        return bi -> new Tuple<>(CollectingBatchIterator.newInstance(bi, collector, numCols), downstream);
    }
}
