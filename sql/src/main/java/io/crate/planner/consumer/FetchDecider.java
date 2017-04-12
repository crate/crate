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

package io.crate.planner.consumer;

/**
 * Component to help make decisions in the planner on how/if fetch-phases should be planned.
 *
 * This is mostly relevant for nested-relations where the plan being created will also contain one or more sub-plans.
 * A planner component for a parent-relation can set a FetchDecider on the {@link ConsumerContext} to influence the planning of
 * child-relations.
 */
@FunctionalInterface
public interface FetchDecider {

    FetchDecider NEVER = () -> false;
    FetchDecider ALWAYS = () -> true;

    /**
     * Verify if it makes sense to attempt doing a fetch-rewrite.
     * This is an approximation, if it returns true it may not be possible to really fetch anything.
     */
    boolean tryFetchRewrite();
}
