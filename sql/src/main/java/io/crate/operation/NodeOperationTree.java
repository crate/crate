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

package io.crate.operation;

import io.crate.planner.node.ExecutionPhase;

import java.util.List;

/**
 * An execution tree generated from a Plan.
 *
 * E.g.
 *
 *      Insert
 *         (MergePhase)
 *         |
 *         +-- Query
 *               (CollectPhase)
 *               (MergePhase)
 *
 * Becomes
 *
 *      NodeOperation (CollectPhase with downstreamNodes pointing to MergePhase)
 *          |
 *      NodeOperation (MergePhase with downstreamNodes pointing to finalMergePhase)
 *          |
 *       finalMergePhase (leaf)
 */
public class NodeOperationTree {

    private final List<NodeOperation> nodeOperations;
    private final ExecutionPhase leaf;
    private final int numLeafUpstreams;

    public NodeOperationTree(List<NodeOperation> nodeOperations, ExecutionPhase leaf, int numLeafUpstreams) {
        this.nodeOperations = nodeOperations;
        this.leaf = leaf;
        this.numLeafUpstreams = numLeafUpstreams;
    }

    /**
     * all NodeOperations (leaf is not included)
     */
    public List<NodeOperation> nodeOperations() {
        return nodeOperations;
    }

    /**
     * the final executionPhase which will provide the final result.
     */
    public ExecutionPhase leaf() {
        return leaf;
    }

    /**
     * number of executionNodes that the parent executionPhases of the leaf have.
     */
    public int numLeafUpstreams() {
        return numLeafUpstreams;
    }
}
