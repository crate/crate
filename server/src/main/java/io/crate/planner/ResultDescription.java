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

import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.execution.dsl.projection.Projection;
import io.crate.types.DataType;

import org.jspecify.annotations.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Describes where a result resides and in which form
 */
public interface ResultDescription {

    /**
     * @return the nodeIds of the nodes that this result is on.
     * If it's empty it is on the handler node.
     */
    Collection<String> nodeIds();

    /**
     * OrderBy information a parent can use to do a sorted-merge.
     * This needs to be set if (and only if) the result is pre-sorted.
     *
     * This DOES NOT require the parent relation to do a FULL sort.
     */
    @Nullable PositionalOrderBy orderBy();

    /**
     * @return Limit that needs to be applied if the result is merged.
     *         This does not indicate if a limit was applied to generate the result,
     *
     *         Therefore if a result was produced by applying the final limit it is allowed to return
     *         {@link LimitAndOffset#NO_LIMIT} to indicate that no additional limit needs to be applied.
     */
    int limit();


    /**
     * @return the max number of rows per node that will be in the result.
     *         -1 for unlimited / unknown.
     */
    int maxRowsPerNode();

    /**
     * @return the offset that needs to be applied after merging.
     */
    int offset();

    /**
     * @return The number of outputs the result has.
     *         This information can be used to strip away additional columns which may be present
     *         if they're required for a sorted-merge.
     *
     *         If a result contains columns which are only required for ordering they must always be
     *         appended on the right of the actual output.
     */
    int numOutputs();

    /**
     * The types of the outputs which will be streamed to whoever is handling this result.
     * This may be larger than {@link #numOutputs()} if the result contains columns which are only relevant for
     * sorting
     */
    List<DataType<?>> streamOutputs();

    /**
     * Indicates if the operations so far are executed beneath/at shard-level
     *
     * This is useful for parent-operators to know so they can add shard-level projections
     * via {@link ExecutionPlan#addProjection(Projection)}
     */
    default boolean executesOnShard() {
        return false;
    }

    default boolean hasRemainingLimitOrOffset() {
        return limit() != LimitAndOffset.NO_LIMIT || offset() != 0;
    }
}
