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

package io.crate.execution.dsl.phases;


import java.util.Collection;

/**
 * An execution tree generated from a Plan.
 * <p>
 * E.g.
 * <p>
 * Insert
 * (MergePhase)
 * |
 * +-- Query
 * (CollectPhase)
 * (MergePhase)
 * <p>
 * Becomes
 * <p>
 * NodeOperation (CollectPhase with downstreamNodes pointing to MergePhase)
 * |
 * NodeOperation (MergePhase with downstreamNodes pointing to finalMergePhase)
 * |
 * finalMergePhase (leaf)
 */
public class NodeOperationTree {

    private final Collection<NodeOperation> nodeOperations;
    private final ExecutionPhase leaf;

    public NodeOperationTree(Collection<NodeOperation> nodeOperations, ExecutionPhase leaf) {
        this.nodeOperations = nodeOperations;
        this.leaf = leaf;
    }

    /**
     * all NodeOperations (leaf is not included)
     */
    public Collection<NodeOperation> nodeOperations() {
        return nodeOperations;
    }

    /**
     * the final executionPhase which will provide the final result.
     */
    public ExecutionPhase leaf() {
        return leaf;
    }

    @Override
    public String toString() {
        return "{NodeOperationTree: {" +
               "nodeOperations: " + nodeOperations +
               ", leaf: " + ExecutionPhases.debugPrint(leaf) +
               "}}";
    }
}
