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

package io.crate.planner.operators;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.statistics.TableStats;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ForwardingLogicalPlan implements LogicalPlan {

    private final List<LogicalPlan> sources;
    final LogicalPlan source;

    public ForwardingLogicalPlan(LogicalPlan source) {
        this.source = source;
        this.sources = List.of(source);
    }

    public LogicalPlan source() {
        return source;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        LogicalPlan newSource = source.pruneOutputsExcept(tableStats, outputsToKeep);
        if (newSource == source) {
            return this;
        }
        return replaceSources(List.of(newSource));
    }

    @Override
    public List<Symbol> outputs() {
        return source.outputs();
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return source.baseTables();
    }

    @Override
    public Set<RelationName> getRelationNames() {
        return source.getRelationNames();
    }

    @Override
    public List<LogicalPlan> sources() {
        return sources;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return source.dependencies();
    }

    @Override
    public long numExpectedRows() {
        return source.numExpectedRows();
    }

    @Override
    public long estimatedRowSize() {
        return source.estimatedRowSize();
    }
}
