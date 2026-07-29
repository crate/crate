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

package io.crate.planner.operators;

public enum PlanHint {
    PREFER_SOURCE_LOOKUP,

    /**
     * Set by a join on the plans it builds for its own sources, to prevent a nested join from being
     * executed as distributed.
     * <p>
     * A distributed join re-shuffles (modulo) its inputs across nodes. If a nested join is also
     * distributed, its output has to be re-shuffled again for the parent join, which makes the two
     * joins mutually dependent across nodes and can deadlock:
     * <ul>
     *   <li>the parent join's receiver only completes a page once it has a bucket from *every*
     *       nested-join instance ({@code CumulativePageBucketReceiver} requires
     *       {@code bucketsByIdx.size() == numBuckets}), and</li>
     *   <li>a nested-join instance that filled its own output page stops consuming its input until
     *       all of its downstreams acknowledged
     *       ({@code DistributingConsumer#countdownAndMaybeContinue} resumes only once
     *       {@code numActiveRequests} reaches 0).</li>
     * </ul>
     * With paging inputs these two barriers can form a cycle: instance A is blocked on its output
     * while still holding an unacknowledged input page, so the shared upstream never sends the next
     * page that instance B needs in order to emit the bucket A's downstream is waiting for.
     * <p>
     * Keeping nested joins non-distributed means a nested join produces a single bucket per
     * downstream input, so the cycle cannot form. The outermost join is unaffected -- nothing above
     * it sets this hint -- so it can still be executed distributed.
     * <p>
     * Because hints are propagated down through every operator's
     * {@code build(...)}, this also covers joins that are not direct children, e.g. a join
     * underneath an {@code Eval} or {@code Rename}.
     */
    AVOID_DISTRIBUTED_JOIN
}
