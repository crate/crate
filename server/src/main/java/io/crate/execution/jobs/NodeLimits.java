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

package io.crate.execution.jobs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.elasticsearch.common.inject.Singleton;

import io.crate.concurrent.limits.ConcurrencyLimit;

/**
 * Tracks concurrency limits per node
 */
@Singleton
public class NodeLimits {

    private final ConcurrencyLimit unknownNodelimit = ConcurrencyLimit.newDefault();
    private final Map<String, ConcurrencyLimit> limitsPerNode = new ConcurrentHashMap<>();

    public ConcurrencyLimit get(@Nullable String nodeId) {
        if (nodeId == null) {
            return unknownNodelimit;
        }
        return limitsPerNode.computeIfAbsent(nodeId, ignored -> ConcurrencyLimit.newDefault());
    }

    public void nodeDisconnected(String nodeId) {
        limitsPerNode.remove(nodeId);
    }

    public long totalNumInflight() {
        return limitsPerNode.values()
            .stream()
            .mapToLong(ConcurrencyLimit::numInflight)
            .sum();
    }
}
