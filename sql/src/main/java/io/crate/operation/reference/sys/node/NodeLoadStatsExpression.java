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

package io.crate.operation.reference.sys.node;


public class NodeLoadStatsExpression extends NestedNodeStatsExpression {

    private static final String ONE = "1";
    private static final String FIVE = "5";
    private static final String FIFTEEN = "15";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";

    public NodeLoadStatsExpression() {
        childImplementations.put(ONE, new LoadStatsExpression(0));
        childImplementations.put(FIVE, new LoadStatsExpression(1));
        childImplementations.put(FIFTEEN, new LoadStatsExpression(2));
        childImplementations.put(PROBE_TIMESTAMP, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                return this.row.extendedOsStats().timestamp();
            }
        });
    }

    private static class LoadStatsExpression extends SimpleNodeStatsExpression<Double> {

        private final int idx;

        LoadStatsExpression(int idx) {
            this.idx = idx;
        }

        @Override
        public Double innerValue() {
            try {
                return this.row.extendedOsStats().loadAverage()[idx];
            } catch (IndexOutOfBoundsException e) {
                return -1d;
            }
        }
    }
}
