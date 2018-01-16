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

package io.crate.execution.expression.reference.sys.node.local;

import io.crate.metadata.ReferenceImplementation;
import io.crate.monitor.ExtendedOsStats;
import io.crate.execution.expression.reference.NestedObjectExpression;

public class NodeLoadExpression extends NestedObjectExpression {

    private static final String ONE = "1";
    private static final String FIVE = "5";
    private static final String FIFTEEN = "15";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";

    public NodeLoadExpression(final ExtendedOsStats os) {
        childImplementations.put(ONE, new LoadExpression(os, 0));
        childImplementations.put(FIVE, new LoadExpression(os, 1));
        childImplementations.put(FIFTEEN, new LoadExpression(os, 2));
        childImplementations.put(PROBE_TIMESTAMP, os::timestamp);
    }

    private static class LoadExpression implements ReferenceImplementation<Double> {

        private final int idx;
        private final ExtendedOsStats stats;

        LoadExpression(ExtendedOsStats stats, int idx) {
            this.idx = idx;
            this.stats = stats;
        }

        @Override
        public Double value() {
            try {
                return stats.loadAverage()[idx];
            } catch (IndexOutOfBoundsException e) {
                return -1d;
            }
        }
    }
}
