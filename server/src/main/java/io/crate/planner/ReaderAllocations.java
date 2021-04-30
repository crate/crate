/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import io.crate.metadata.RelationName;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class ReaderAllocations {

    private final TreeMap<Integer, String> readerIndices = new TreeMap<>();
    private final Map<String, IntSet> nodeReaders = new HashMap<>();
    private final TreeMap<String, Integer> bases;
    private final Map<RelationName, Collection<String>> tableIndices;
    private final Map<String, RelationName> indicesToIdents;

    ReaderAllocations(TreeMap<String, Integer> bases,
                      Map<String, Map<Integer, String>> shardNodes,
                      Map<RelationName, Collection<String>> tableIndices) {
        this.bases = bases;
        this.tableIndices = tableIndices;
        this.indicesToIdents = new HashMap<>(tableIndices.values().size());
        for (Map.Entry<RelationName, Collection<String>> entry : tableIndices.entrySet()) {
            for (String index : entry.getValue()) {
                indicesToIdents.put(index, entry.getKey());
            }
        }
        for (Map.Entry<String, Integer> entry : bases.entrySet()) {
            readerIndices.put(entry.getValue(), entry.getKey());
        }
        for (Map.Entry<String, Map<Integer, String>> entry : shardNodes.entrySet()) {
            Integer base = bases.get(entry.getKey());
            if (base == null) {
                continue;
            }
            for (Map.Entry<Integer, String> nodeEntries : entry.getValue().entrySet()) {
                int readerId = base + nodeEntries.getKey();
                IntSet readerIds = nodeReaders.get(nodeEntries.getValue());
                if (readerIds == null) {
                    readerIds = new IntHashSet();
                    nodeReaders.put(nodeEntries.getValue(), readerIds);
                }
                readerIds.add(readerId);
            }
        }
    }

    public Map<RelationName, Collection<String>> tableIndices() {
        return tableIndices;
    }

    public TreeMap<Integer, String> indices() {
        return readerIndices;
    }

    public Map<String, IntSet> nodeReaders() {
        return nodeReaders;
    }

    public TreeMap<String, Integer> bases() {
        return bases;
    }

    public Map<String, RelationName> indicesToIdents() {
        return indicesToIdents;
    }
}
