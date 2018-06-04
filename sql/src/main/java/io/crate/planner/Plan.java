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

package io.crate.planner;

import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.operators.SubQueryResults;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Representation of a complete top-level plan which can be consumed by an {@link io.crate.executor.Executor}.
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
        /**
         * ALL is used in {@link io.crate.beans.QueryStats} as a key to hold the aggregation of all other query types.
         * This type must never be used to classify a plan.
         */
        ALL,
    }

    StatementType type();

    void execute(DependencyCarrier executor,
                 PlannerContext plannerContext,
                 RowConsumer consumer,
                 Row params,
                 SubQueryResults subQueryResults);

    default List<CompletableFuture<Long>> executeBulk(DependencyCarrier executor,
                                                      PlannerContext plannerContext,
                                                      List<Row> bulkParams,
                                                      SubQueryResults subQueryResults) {
        throw new UnsupportedOperationException(
            "Bulk operation not supported for " + this.getClass().getSimpleName());
    }
}
