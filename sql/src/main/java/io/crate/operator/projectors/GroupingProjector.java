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

package io.crate.operator.projectors;

import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationCollector;
import io.crate.operator.aggregation.AggregationState;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.operations.ImplementationSymbolVisitor;

import java.util.*;

public class GroupingProjector implements Projector {

    private final Input[] keyInputs;
    private final CollectExpression[] collectExpressions;

    private final Map<List<Object>, AggregationState[]> result;
    private final AggregationCollector[] aggregationCollectors;

    private Object[][] rows;
    private Projector upStream = null;

    public GroupingProjector(Input[] keyInputs,
                             CollectExpression[] collectExpressions,
                             ImplementationSymbolVisitor.AggregationContext[] aggregations) {
        this.keyInputs = keyInputs;
        this.collectExpressions = collectExpressions;

        this.aggregationCollectors = new AggregationCollector[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            aggregationCollectors[i] = new AggregationCollector(
                    aggregations[i].symbol(),
                    aggregations[i].function(),
                    aggregations[i].inputs()
            );
        }

        result = new HashMap<>();
    }

    @Override
    public void setUpStream(Projector upStream) {
        this.upStream = upStream;
    }

    @Override
    public void startProjection() {
        for (CollectExpression collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }
    }

    @Override
    public synchronized boolean setNextRow(final Object... row) {
        for (CollectExpression collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }

        // TODO: use something with better equals() performance for the keys
        List<Object> key = new ArrayList<>(keyInputs.length);
        for (Input keyInput : keyInputs) {
            key.add(keyInput.value());
        }

        AggregationState[] states = result.get(key);
        if (states == null) {
            states = new AggregationState[aggregationCollectors.length];
            for (int i = 0; i < aggregationCollectors.length; i++) {
                aggregationCollectors[i].startCollect();
                aggregationCollectors[i].processRow();
                states[i] = aggregationCollectors[i].state();
            }
            result.put(key, states);
        } else {
            for (int i = 0; i < aggregationCollectors.length; i++) {
                aggregationCollectors[i].state(states[i]);
                aggregationCollectors[i].processRow();
            }
        }

        return true;
    }

    @Override
    public void finishProjection() {
        rows = new Object[result.size()][keyInputs.length + aggregationCollectors.length];
        boolean sendToUpstream = upStream != null;
        if (sendToUpstream) {
            upStream.startProjection();
        }

        int r = 0;
        for (Map.Entry<List<Object>, AggregationState[]> entry : result.entrySet()) {
            Object[] row = rows[r];
            transformToRow(entry, row);
            if (sendToUpstream) {
                sendToUpstream = upStream.setNextRow(row);
            }
            r++;
        }

        if (upStream != null) {
            upStream.finishProjection();
        }
    }

    /**
     * transform map entry into pre-allocated object array.
     * @param entry
     * @param row
     */
    private static void transformToRow(Map.Entry<List<Object>, AggregationState[]> entry, Object[] row) {
        int c = 0;

        for (Object o : entry.getKey()) {
            row[c] = o;
            c++;
        }
        for (AggregationState aggregationState : entry.getValue()) {
            row[c] = aggregationState.value();
            c++;
        }
    }

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return rows;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new EntryToRowIterator(result.entrySet().iterator(), keyInputs.length + aggregationCollectors.length);
    }

    private static class EntryToRowIterator implements Iterator<Object[]> {

        private final Iterator<Map.Entry<List<Object>, AggregationState[]>> iter;
        private final int rowLength;

        private EntryToRowIterator(Iterator<Map.Entry<List<Object>, AggregationState[]>> iter,
                                   int rowLength) {
            this.iter = iter;
            this.rowLength = rowLength;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Object[] next() {
            Map.Entry<List<Object>, AggregationState[]> entry = iter.next();
            Object[] row = new Object[rowLength];
            transformToRow(entry, row);
            return row;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }
}
