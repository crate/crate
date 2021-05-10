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

/**
 * <p>
 *  This package contains classes for the execution domain-specific language (DSL).
 *  This DSL is used to describe how queries are executed.
 *  <br />
 *
 *  "execution" here refers to the *generic* execution framework used to
 *  execute DQL and most DML statements.
 *  (Not all DML statements are executed using the generic execution framework;
 *  For example delete-by-id has a specialized/optimized execution implementation)
 *  Similarly, DDL statements are executed using specific implementations.
 *  <br />
 * </p>
 *
 * <h2>Phases</h2>
 *
 * <p>
 *  A ExecutionPhase is the "core" of the execution DSL. The interfaces are:
 * </p>
 * <ul>
 *  <li>{@link io.crate.execution.dsl.phases.ExecutionPhase}</li>
 *  <li>{@link io.crate.execution.dsl.phases.UpstreamPhase}</li>
 * </ul>
 * <p>
 *  There are three categories of ExecutionPhases:
 * </p>
 *
 * <ul>
 *  <li>
 *      Phases that describe *how* to produce *what* data<br />
 *      ({@link io.crate.execution.dsl.phases.CollectPhase})
 *  </li>
 *  <li>
 *      Phases that describe how to receive and process data
 *      ({@link io.crate.execution.dsl.phases.MergePhase}, {@link io.crate.execution.dsl.phases.JoinPhase})
 *  </li>
 *  <li>
 *      Phases that instruct the executor to create some kind of "context" which may be used by other phases.
 *      ({@link io.crate.execution.dsl.phases.FetchPhase})
 *  </li>
 * </ul>
 *
 * <p>
 *  Data generally "flows through" phases. For example:
 *  (Context phases are an exception here)
 * </p>
 * <pre>
 *     select * from t1
 *
 *     Would be represented as
 *
 *     CollectPhase (Running on [node1, node2, node3])
 *          |
 *     MergePhase (Running on [node1])
 *
 *     The data flows from CollectPhase to MergePhase
 * </pre>
 * <p>
 *     This data flow is implicitly defined in a concrete {@link io.crate.planner.ExecutionPlan} (e.g. {@link io.crate.planner.Merge})
 *     due to the order how the phases appear in the plan.
 *     <br />
 *     {@link io.crate.execution.dsl.phases.NodeOperation} is a wrapped ExecutionPhase annotated with information
 *     regarding the next/downstream ExecutionPhase, which should receive the data.
 *
 *     This is used primarily because nodes participating in the execution of an operation described by the DSL may
 *     only receive a subset of the full execution-description.
 *     (In the example above, nodes node2 and node3 don't have to have access to the MergePhase running on node1,
 *     but they *do* need to know that they're supposed to send their partial results to node1)
 *
 *     The "last" (sometimes referred to as leaf) executionPhase is expected to contain the final result for a client.
 * </p>
 *
 * <p>
 *  Each phase usually results in 1 {@link io.crate.execution.jobs.Task} being created.
 *  These contexts contain the execution logic.
 *  (There may be some exceptions to this -
 *  e.g. multiple phases may be merged at execution time into 1 Task for optimization purposes)
 * </p>
 *
 *
 * <h2>Projections</h2>
 *
 * <p>
 *  Projections form the second part of the execution DSL.
 *  Implementations of {@link io.crate.execution.dsl.phases.ExecutionPhase}
 *  may contain 1 or more {@link io.crate.execution.dsl.projection.Projection}s which describe "data transformations".
 *  This can include things like filters, limits, but it also includes transformations with side-effects (e.g. update or delete)
 * </p>
 */
package io.crate.execution.dsl;
