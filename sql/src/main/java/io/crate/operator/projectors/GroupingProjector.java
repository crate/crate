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
import io.crate.operator.operations.AggregationContext;

import java.util.*;

public class GroupingProjector implements Projector {

    private final CollectExpression[] collectExpressions;

    private final Grouper grouper;

    private Object[][] rows;
    private Projector downStream = null;

    public GroupingProjector(Input[] keyInputs,
                             CollectExpression[] collectExpressions,
                             AggregationContext[] aggregations) {
        this.collectExpressions = collectExpressions;

        AggregationCollector[] aggregationCollectors = new AggregationCollector[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            aggregationCollectors[i] = new AggregationCollector(
                    aggregations[i].symbol(),
                    aggregations[i].function(),
                    aggregations[i].inputs()
            );
        }

        if (keyInputs.length == 1) {
            grouper = new SingleKeyGrouper(keyInputs, collectExpressions, aggregationCollectors);
        } else {
            grouper = new ManyKeyGrouper(keyInputs, collectExpressions, aggregationCollectors);
        }
    }

    @Override
    public void setDownStream(Projector downStream) {
        this.downStream = downStream;
    }

    @Override
    public void startProjection() {
        for (CollectExpression collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }
    }

    @Override
    public synchronized boolean setNextRow(final Object... row) {
        return grouper.setNextRow(row);
    }

    @Override
    public void finishProjection() {
        rows = grouper.finish();
    }

    /**
     * transform map entry into pre-allocated object array.
     * @param entry
     * @param row
     * @param aggregationCollectors
     */
    private static void transformToRow(Map.Entry<List<Object>, AggregationState[]> entry,
                                       Object[] row,
                                       AggregationCollector[] aggregationCollectors) {
        int c = 0;

        for (Object o : entry.getKey()) {
            row[c] = o;
            c++;
        }

        AggregationState[] aggregationStates = entry.getValue();
        for (int i = 0; i < aggregationStates.length; i++) {
            aggregationCollectors[i].state(aggregationStates[i]);
            row[c] = aggregationCollectors[i].finishCollect();
            c++;
        }
    }

    private static void singleTransformToRow(Map.Entry<Object, AggregationState[]> entry,
                                       Object[] row,
                                       AggregationCollector[] aggregationCollectors) {
        int c = 0;
        row[c] = entry.getKey();
        c++;
        AggregationState[] aggregationStates = entry.getValue();
        for (int i = 0; i < aggregationStates.length; i++) {
            aggregationCollectors[i].state(aggregationStates[i]);
            row[c] = aggregationCollectors[i].finishCollect();
            c++;
        }
    }

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return rows;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return grouper.iterator();
    }

    private interface Grouper {
        boolean setNextRow(final Object... row);
        Object[][] finish();
        Iterator<Object[]> iterator();
    }

    private class SingleKeyGrouper implements Grouper {

        private final Map<Object, AggregationState[]> result;
        private final CollectExpression[] collectExpressions;
        private final AggregationCollector[] aggregationCollectors;
        private final Input keyInput;

        public SingleKeyGrouper(Input[] keyInputs,
                                CollectExpression[] collectExpressions,
                                AggregationCollector[] aggregationCollectors) {
            this.result = new HashMap<>();
            this.keyInput = keyInputs[0];
            this.collectExpressions = collectExpressions;
            this.aggregationCollectors = aggregationCollectors;
        }

        @Override
        public boolean setNextRow(Object... row) {
            for (CollectExpression collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
            }

            Object key = keyInput.value();
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
        public Object[][] finish() {
            Object[][] rows = new Object[result.size()][1 + aggregationCollectors.length];
            boolean sendToDownStream = downStream != null;
            if (sendToDownStream) {
                downStream.startProjection();
            }

            int r = 0;
            for (Map.Entry<Object, AggregationState[]> entry : result.entrySet()) {
                Object[] row = rows[r];
                singleTransformToRow(entry, row, aggregationCollectors);
                if (sendToDownStream) {
                    sendToDownStream = downStream.setNextRow(row);
                }
                r++;
            }

            if (downStream != null) {
                downStream.finishProjection();
            }

            return rows;
        }

        @Override
        public Iterator<Object[]> iterator() {
            return new SingleEntryToRowIterator(
                    result.entrySet().iterator(), aggregationCollectors.length + 1, aggregationCollectors);
        }
    }

    private class ManyKeyGrouper implements Grouper {

        private final AggregationCollector[] aggregationCollectors;
        private final Map<List<Object>, AggregationState[]> result;
        private final CollectExpression[] collectExpressions;
        private final Input[] keyInputs;

        public ManyKeyGrouper(Input[] keyInputs,
                              CollectExpression[] collectExpressions,
                              AggregationCollector[] aggregationCollectors) {
            this.result = new HashMap<>();
            this.collectExpressions = collectExpressions;
            this.keyInputs = keyInputs;
            this.aggregationCollectors = aggregationCollectors;
        }

        @Override
        public boolean setNextRow(Object... row) {
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
        public Object[][] finish() {
            Object[][] rows = new Object[result.size()][keyInputs.length + aggregationCollectors.length];
            boolean sendToDownStream = downStream != null;
            if (sendToDownStream) {
                downStream.startProjection();
            }

            int r = 0;
            for (Map.Entry<List<Object>, AggregationState[]> entry : result.entrySet()) {
                Object[] row = rows[r];
                transformToRow(entry, row, aggregationCollectors);
                if (sendToDownStream) {
                    sendToDownStream = downStream.setNextRow(row);
                }
                r++;
            }

            if (downStream != null) {
                downStream.finishProjection();
            }

            return rows;
        }

        @Override
        public Iterator<Object[]> iterator() {
            return new MultiEntryToRowIterator(
                    result.entrySet().iterator(),
                    keyInputs.length + aggregationCollectors.length,
                    aggregationCollectors);
        }
    }


    private static class SingleEntryToRowIterator implements Iterator<Object[]> {

        private final Iterator<Map.Entry<Object, AggregationState[]>> iter;
        private final int rowLength;
        private final AggregationCollector[] aggregationCollectors;

        private SingleEntryToRowIterator(Iterator<Map.Entry<Object, AggregationState[]>> iter,
                                   int rowLength, AggregationCollector[] aggregationCollectors) {
            this.iter = iter;
            this.rowLength = rowLength;
            this.aggregationCollectors = aggregationCollectors;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Object[] next() {
            Map.Entry<Object, AggregationState[]> entry = iter.next();
            Object[] row = new Object[rowLength];
            singleTransformToRow(entry, row, aggregationCollectors);
            return row;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }

    private static class MultiEntryToRowIterator implements Iterator<Object[]> {

        private final Iterator<Map.Entry<List<Object>, AggregationState[]>> iter;
        private final int rowLength;
        private final AggregationCollector[] aggregationCollectors;

        private MultiEntryToRowIterator(Iterator<Map.Entry<List<Object>, AggregationState[]>> iter,
                                   int rowLength, AggregationCollector[] aggregationCollectors) {
            this.iter = iter;
            this.rowLength = rowLength;
            this.aggregationCollectors = aggregationCollectors;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Object[] next() {
            Map.Entry<List<Object>, AggregationState[]> entry = iter.next();
            Object[] row = new Object[rowLength];
            transformToRow(entry, row, aggregationCollectors);
            return row;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }
}
