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

package io.crate.planner;

import io.crate.planner.node.PlanNode;
import org.cratedb.DataType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Plan implements Iterable<PlanNode> {

    private ArrayList<PlanNode> nodes = new ArrayList<>();
    private boolean expectsAffectedRows = false;

    public void add(PlanNode node) {
        nodes.add(node);
    }

    @Override
    public Iterator<PlanNode> iterator() {
        return nodes.iterator();
    }

    public DataType[] finalOutputTypes() {
        if (nodes.size() == 0) {
            return new DataType[0];
        } else {
            List<DataType> types = nodes.get(nodes.size() - 1).outputTypes();
            return types.toArray(new DataType[types.size()]);
        }
    }

    public void expectsAffectedRows(boolean expectsAffectedRows) {
        this.expectsAffectedRows = expectsAffectedRows;
    }

    public boolean expectsAffectedRows() {
        return expectsAffectedRows;
    }
}
