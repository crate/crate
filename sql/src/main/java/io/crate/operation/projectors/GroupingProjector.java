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
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.*;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeValue;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class GroupingProjector implements Projector, RowDownstreamHandle {

    private final CollectExpression[] collectExpressions;

    private static final ESLogger logger = Loggers.getLogger(GroupingProjector.class);
    private final RamAccountingContext ramAccountingContext;

    private Grouper grouper;
    private RowDownstreamHandle downstream;
    private AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    public GroupingProjector(List<? extends DataType> keyTypes,
                             List<Input<?>> keyInputs,
                             CollectExpression[] collectExpressions,
                             AggregationContext[] aggregations,
                             RamAccountingContext ramAccountingContext) {
        assert keyTypes.size() == keyInputs.size() : "number of key types must match with number of key inputs";
        assert allTypesKnown(keyTypes) : "must have a known type for each key input";
        this.collectExpressions = collectExpressions;
        this.ramAccountingContext = ramAccountingContext;

        Aggregator[] aggregators = new Aggregator[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            aggregators[i] = new Aggregator(
                    ramAccountingContext,
                    aggregations[i].symbol(),
                    aggregations[i].function(),
                    aggregations[i].inputs()
            );
        }

        // grouper object size overhead
        ramAccountingContext.addBytes(8);
        if (keyInputs.size() == 1) {
            grouper = new SingleKeyGrouper(keyInputs.get(0), keyTypes.get(0),
                    collectExpressions, aggregators);
        } else {
            grouper = new ManyKeyGrouper(keyInputs, keyTypes,
                    collectExpressions, aggregators);
        }
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
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void startProjection() {
        for (CollectExpression collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }

        if (remainingUpstreams.get() <= 0) {
            finish();
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        try {
            return grouper.setNextRow(row);
        } catch (CircuitBreakingException e) {
            if (downstream != null) {
                downstream.fail(e);
                downstream = null;
            }
            throw e;
        }
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() == 0) {
            if (grouper != null) {
                grouper.finish();
                cleanUp();
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("grouping operation size is: {}", new ByteSizeValue(ramAccountingContext.totalBytes()));
        }
    }

    @Override
    public void fail(Throwable throwable) {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            if (downstream != null) {
                downstream.fail(throwable);
            }
            cleanUp();
            return;
        }
        failure.set(throwable);
    }

    /**
     * transform map entry into pre-allocated object array.
     */
    private static void transformToRow(Map.Entry<List<Object>, Object[]> entry,
                                       Object[] row,
                                       Aggregator[] aggregators) {
        int c = 0;

        for (Object o : entry.getKey()) {
            row[c] = o;
            c++;
        }

        Object[] states = entry.getValue();
        for (int i = 0; i < states.length; i++) {
            row[c] = aggregators[i].finishCollect(states[i]);
            c++;
        }
    }

    private static void singleTransformToRow(Map.Entry<Object, Object[]> entry,
                                             Object[] row,
                                             Aggregator[] aggregators) {
        int c = 0;
        row[c] = entry.getKey();
        c++;
        Object[] states = entry.getValue();
        for (int i = 0; i < states.length; i++) {
            row[c] = aggregators[i].finishCollect(states[i]);
            c++;
        }
    }

    private void cleanUp() {
        grouper = null;
    }

    private interface Grouper {
        boolean setNextRow(final Row row);

        void finish();
    }

    private class SingleKeyGrouper implements Grouper {

        private final Map<Object, Object[]> result;
        private final Aggregator[] aggregators;
        private final Input keyInput;
        private final CollectExpression[] collectExpressions;
        private final SizeEstimator<Object> sizeEstimator;

        public SingleKeyGrouper(Input keyInput,
                                DataType keyInputType,
                                CollectExpression[] collectExpressions,
                                Aggregator[] aggregators) {
            this.collectExpressions = collectExpressions;
            this.result = new HashMap<>();
            this.keyInput = keyInput;
            this.aggregators = aggregators;
            sizeEstimator = SizeEstimatorFactory.create(keyInputType);
        }

        @Override
        public boolean setNextRow(Row row) {
            for (CollectExpression collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }

            Object key = keyInput.value();

            // HashMap.get requires some objects (iterators) and at least 2 integers
            ramAccountingContext.addBytes(32);
            Object[] states = result.get(key);
            if (states == null) {
                states = new Object[aggregators.length];
                for (int i = 0; i < aggregators.length; i++) {
                    Object state = aggregators[i].prepareState();
                    states[i] = aggregators[i].processRow(state);
                }
                ramAccountingContext.addBytes(
                        RamAccountingContext.roundUp(sizeEstimator.estimateSize(key)) + 24); // 24 bytes overhead per entry
                result.put(key, states);
            } else {
                for (int i = 0; i < aggregators.length; i++) {
                    states[i] = aggregators[i].processRow(states[i]);
                }
            }

            return true;
        }

        @Override
        public void finish() {
            if (downstream == null) {
                return;
            }
            Throwable throwable = failure.get();
            if (throwable != null) {
                downstream.fail(throwable);
            }

            // TODO: check ram accounting
            // account the multi-dimension `rows` array
            // 1st level
            ramAccountingContext.addBytes(RamAccountingContext.roundUp(12 + result.size() * 4));
            // 2nd level
            ramAccountingContext.addBytes(RamAccountingContext.roundUp(
                    (1 + aggregators.length) * 4 + 12));
            RowN row = new RowN(1 + aggregators.length);
            for (Map.Entry<Object, Object[]> entry : result.entrySet()) {
                Object[] cells = new Object[row.size()];
                singleTransformToRow(entry, cells, aggregators);
                row.cells(cells);
                if (!downstream.setNextRow(row)) {
                    break;
                }
            }
            downstream.finish();
        }
    }

    private class ManyKeyGrouper implements Grouper {

        private final Aggregator[] aggregators;
        private final Map<List<Object>, Object[]> result;
        private final List<Input<?>> keyInputs;
        private final CollectExpression[] collectExpressions;
        private final List<SizeEstimator<Object>> sizeEstimators;

        public ManyKeyGrouper(List<Input<?>> keyInputs,
                              List<? extends DataType> keyTypes,
                              CollectExpression[] collectExpressions,
                              Aggregator[] aggregators) {
            this.collectExpressions = collectExpressions;
            this.result = new HashMap<>();
            this.keyInputs = keyInputs;
            this.aggregators = aggregators;
            sizeEstimators = new ArrayList<>(keyTypes.size());
            for (DataType dataType : keyTypes) {
                sizeEstimators.add(SizeEstimatorFactory.create(dataType));
            }
        }

        @Override
        public boolean setNextRow(Row row) {
            for (CollectExpression collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }

            // key list ram accounting
            ramAccountingContext.addBytes(12);
            // TODO: use something with better equals() performance for the keys
            List<Object> key = new ArrayList<>(keyInputs.size());
            int keyIdx = 0;
            for (Input keyInput : keyInputs) {
                key.add(keyInput.value());
                // 4 bytes overhead per list entry + 4 bytes overhead for later hashCode
                // calculation while using list.get()
                ramAccountingContext.addBytes(RamAccountingContext.roundUp(
                        sizeEstimators.get(keyIdx).estimateSize(keyInput.value()) + 4) + 4);
                keyIdx++;
            }

            // HashMap.get requires some objects (iterators) and at least 2 integers
            ramAccountingContext.addBytes(32);
            Object[] states = result.get(key);
            if (states == null) {
                states = new Object[aggregators.length];
                for (int i = 0; i < aggregators.length; i++) {
                    Object state = aggregators[i].prepareState();
                    state = aggregators[i].processRow(state);
                    states[i] = state;
                }
                ramAccountingContext.addBytes(24); // 24 bytes overhead per map entry
                result.put(key, states);
            } else {
                for (int i = 0; i < aggregators.length; i++) {
                    states[i] = aggregators[i].processRow(states[i]);
                }
            }

            return true;
        }

        @Override
        public void finish() {
            if (downstream == null){
                return;
            }
            Throwable throwable = failure.get();
            if (throwable != null) {
                downstream.fail(throwable);
            }
            // account the multi-dimension `rows` array
            // 1st level
            ramAccountingContext.addBytes(RamAccountingContext.roundUp(12 + result.size() * 4));
            // 2nd level
            ramAccountingContext.addBytes(RamAccountingContext.roundUp(12 +
                    (keyInputs.size() + aggregators.length) * 4));
            //Object[][] rows = new Object[result.size()][keyInputs.size() + aggregators.length];
            RowN row = new RowN(keyInputs.size() + aggregators.length);
            for (Map.Entry<List<Object>, Object[]> entry : result.entrySet()) {
                Object[] cells = new Object[row.size()];
                transformToRow(entry, cells, aggregators);
                row.cells(cells);
                if (!downstream.setNextRow(row)){
                    return;
                }
            }
            downstream.finish();
        }
    }
}
