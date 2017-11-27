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

import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.ESGet;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;

public class Get implements LogicalPlan {

    public static LogicalPlan create(QueriedDocTable table) {
        QuerySpec querySpec = table.querySpec();
        Optional<DocKeys> optKeys = querySpec.where().docKeys();
        assert !querySpec.hasAggregates() : "Can't create Get logical plan for queries with aggregates";
        assert querySpec.groupBy().isEmpty() : "Can't create Get logical plan for queries with group by";
        assert optKeys.isPresent() : "Can't create Get logial plan without docKeys";

        DocKeys docKeys = optKeys.get();
        if (docKeys.withVersions()) {
            throw new VersionInvalidException();
        }
        table.tableRelation().validateOrderBy(querySpec.orderBy());
        return new Get(table, docKeys, querySpec.outputs(), querySpec.limit(), querySpec.offset());
    }

    private final DocTableRelation tableRelation;
    private final QuerySpec querySpec;
    private final DocKeys docKeys;
    private final List<Symbol> outputs;
    private final Symbol limit;
    private final Symbol offset;

    private Get(QueriedDocTable table, DocKeys docKeys, List<Symbol> outputs, Symbol limit, Symbol offset) {
        this.tableRelation = table.tableRelation();
        this.querySpec = table.querySpec();
        this.docKeys = docKeys;
        this.outputs = outputs;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public ESGet build(PlannerContext plannerContext,
                       ProjectionBuilder projectionBuilder,
                       int limitHint,
                       int offsetHint,
                       @Nullable OrderBy order,
                       @Nullable Integer pageSizeHint,
                       Row params,
                       Map<SelectSymbol, Object> subQueryValues) {
        int limit = firstNonNull(plannerContext.toInteger(this.limit), LogicalPlanner.NO_LIMIT);
        int offset = firstNonNull(plannerContext.toInteger(this.offset), 0);
        return new ESGet(
            plannerContext.nextExecutionPhaseId(),
            tableRelation.tableInfo(),
            outputs,
            docKeys,
            querySpec.orderBy(),
            limit,
            offset
        );
    }

    @Override
    public LogicalPlan tryCollapse() {
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return Collections.emptyMap();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return Collections.singletonList(tableRelation);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Collections.emptyMap();
    }

    @Override
    public long numExpectedRows() {
        return docKeys.size();
    }
}
