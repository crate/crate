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

    /// Indicates that the next Join operator must not build a distributed execution plan.
    /// Used to avoid a deadlock. For example, a logical plan like:
    ///
    /// ```
    ///   t1    t2
    ///     \  /
    ///     join  t3
    ///       \   /
    ///       join
    /// ```
    ///
    /// Would result in an execution plan as follows without this hint:
    /// - numeric node prefixes are the phase ids
    /// - n1 and n2 are the nodes involved
    ///
    /// ```
    ///      t1             t2                  t3
    ///   0-collect      1-collect          5-collect
    ///    n1   n2        n1   n2            n1    n2
    ///
    ///
    ///    n1   n2       n1   n2
    ///   2-l-merge     3-r-merge
    ///         4-h-join (t1<->t2)
    ///           n1   n2
    ///
    ///
    ///              n1   n2       n1   n2
    ///             6-l-merge    7-r-merge
    ///                 8-h-join
    ///
    /// ```
    ///
    /// This deadlocks for example if:
    ///
    /// - t3/5-collect, t2/1-collect, 3-r-merge, 7-r-merge all complete in a single page and are unproblematic
    /// - n1 and n2 of 0-collect produce a full page and send it to n1 and n2 of 2-l-merge
    /// - n1 of 2-l-merge/4-h-join produces a full page and sends it to n1 and n2 of 6-l-merge
    /// - n2 of 2-l-merge/4-h-join does _not_ produce a full page and requests more data from n1 and n2 of 0-collect
    /// - n2 of 0-collect waits for a result response from n1 of 2-l-merge
    /// - n1 of 2-l-merge doesn't send a result response until it knows if it
    ///   needs more data, which it won't until it's downstream communicates with it
    /// - 6-l-merge waits for data from n2, which it won't get because n2/4-h-join is waiting for data
    AVOID_DISTRIBUTED_JOIN
}
