/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.statistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;

public class StubStatsService implements StatsService {

    private final ConcurrentHashMap<RelationName, Stats> tableStats = new ConcurrentHashMap<>();

    @Override
    public void add(Map<RelationName, Stats> stats) {
        tableStats.putAll(stats);
    }

    @Override
    public Stats getOrDefault(RelationName relationName, Stats defaultValue) {
        return tableStats.getOrDefault(relationName, defaultValue);
    }

    @Nullable
    @Override
    public  Stats get(RelationName relationName) {
        return tableStats.get(relationName);
    }

    @Override
    public void clear() {
        tableStats.clear();
    }

}
