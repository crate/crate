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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dml.BulkResponse;
import io.crate.planner.operators.SubQueryResults;

/**
 * Representation of a complete top-level plan which can be consumed by an {@link io.crate.execution.MultiPhaseExecutor}.
 */
public interface Plan {

    enum StatementType {
        INSERT,
        SELECT,
        UPDATE,
        DELETE,
        COPY,
        DDL,
        MANAGEMENT,
        UNDEFINED,
    }

    StatementType type();

    /**
     * Execute the given plan.
     * Implementations are allowed to raise errors instead of triggering the consumer.
     *
     * Users of the Plan should prefer {@link #execute(DependencyCarrier, PlannerContext, RowConsumer, Row, SubQueryResults)}
     * to ensure the consumer is always invoked.
     */
    void executeOrFail(DependencyCarrier dependencies,
                       PlannerContext plannerContext,
                       RowConsumer consumer,
                       Row params,
                       SubQueryResults subQueryResults) throws Exception;

    /**
     * Execute the plan, transferring the result to the RowConsumer
     *
     * Implementations must override {@link #executeOrFail(DependencyCarrier, PlannerContext, RowConsumer, Row, SubQueryResults)} instead.
     */
    default void execute(DependencyCarrier dependencies,
                         PlannerContext plannerContext,
                         RowConsumer consumer,
                         Row params,
                         SubQueryResults subQueryResults) {
        try {
            executeOrFail(dependencies, plannerContext, consumer, params, subQueryResults);
        } catch (Throwable t) {
            consumer.accept(null, t);
        }
    }

    default CompletableFuture<BulkResponse> executeBulk(DependencyCarrier executor,
                                                        PlannerContext plannerContext,
                                                        List<Row> bulkParams,
                                                        SubQueryResults subQueryResults) {
        throw new UnsupportedOperationException(
            "Bulk operation not supported for " + this.getClass().getSimpleName());
    }
}
