/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.sys.node.fs;

import io.crate.monitor.ExtendedFsStats;
import io.crate.operation.reference.sys.node.DiscoveryNodeContext;
import io.crate.operation.reference.sys.node.NestedDiscoveryNodeExpression;
import io.crate.operation.reference.sys.node.SimpleDiscoveryNodeExpression;

import java.util.HashMap;
import java.util.Map;

import static io.crate.operation.reference.sys.node.fs.NodeFsExpression.*;

public class NodeFsTotalExpression extends NestedDiscoveryNodeExpression {

    private Map<String, Long> totals;

    protected NodeFsTotalExpression() {
        childImplementations.put(SIZE, new NodeFSTotalChildExpression(SIZE));
        childImplementations.put(USED, new NodeFSTotalChildExpression(USED));
        childImplementations.put(AVAILABLE, new NodeFSTotalChildExpression(AVAILABLE));
        childImplementations.put(READS, new NodeFSTotalChildExpression(READS));
        childImplementations.put(BYTES_READ, new NodeFSTotalChildExpression(BYTES_READ));
        childImplementations.put(WRITES, new NodeFSTotalChildExpression(WRITES));
        childImplementations.put(BYTES_WRITTEN, new NodeFSTotalChildExpression(BYTES_WRITTEN));
    }

    @Override
    public void setNextRow(DiscoveryNodeContext row) {
        super.setNextRow(row);
        totals = getTotals();
    }

    private Map<String, Long> getTotals() {
        Map<String, Long> totals = new HashMap<>();
        ExtendedFsStats.Info totalInfo = this.row.extendedFsStats.total();
        totals.put(SIZE, totalInfo.total() == -1 ? -1 : totalInfo.total() * 1024);
        totals.put(USED, totalInfo.used() == -1 ? - 1 : totalInfo.used() * 1024);
        totals.put(AVAILABLE, totalInfo.available() == -1 ? -1 : totalInfo.available() * 1024);
        totals.put(READS, totalInfo.diskReads());
        totals.put(BYTES_READ, totalInfo.diskReadSizeInBytes());
        totals.put(WRITES, totalInfo.diskWrites());
        totals.put(BYTES_WRITTEN, totalInfo.diskWriteSizeInBytes());
        return totals;
    }

    protected class NodeFSTotalChildExpression extends SimpleDiscoveryNodeExpression<Long> {

        private final String name;

        protected NodeFSTotalChildExpression(String name) {
            this.name = name;
        }

        @Override
        public Long innerValue() {
            return totals.get(name);
        }
    }
}
