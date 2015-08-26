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

package io.crate.planner.fetch;

import io.crate.metadata.Routing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IndexBaseVisitor extends Routing.RoutingLocationVisitor {

    private final TreeMap<String, Integer> bases = new TreeMap<>();

    @Override
    public boolean visitIndex(String nodeId, String index, List<Integer> shardIds) {
        Integer currentMax = bases.get(index);
        Integer newMax = Collections.max(shardIds);
        if (currentMax == null || currentMax < newMax) {
            bases.put(index, newMax);
        }
        return true;
    }

    public TreeMap<String, Integer> build() {
        Integer currentBase = 0;
        for (Map.Entry<String, Integer> entry : bases.entrySet()) {
            Integer maxId = entry.getValue();
            entry.setValue(currentBase);
            currentBase += maxId + 1;
        }
        return bases;
    }
}
