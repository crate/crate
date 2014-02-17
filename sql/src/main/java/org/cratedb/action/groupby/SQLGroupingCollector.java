/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action.groupby;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.collect.CollectorExpression;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.GroupTree;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Collector that can be used to get results from a Lucene query.
 * <p/>
 * The result is partitioned by the reducers and grouped by the group key(s)
 * See {@link org.cratedb.action.TransportDistributedSQLAction} for a full overview of the process.
 */
public class SQLGroupingCollector extends Collector {

    protected final ParsedStatement parsedStatement;
    private final AggFunction[] aggFunctions;
    private final List<CollectorExpression> collectorExpressions;
    private final CollectorContext context;
    protected final Rows rows;
    protected final int numReducers;
    private SQLGroupingAggregateHandler aggregateHandler;

    public SQLGroupingCollector(
            ParsedStatement parsedStatement,
            CollectorContext context,
            Map<String, AggFunction> aggFunctionMap,
            int numReducers) {
        this.parsedStatement = parsedStatement;
        this.numReducers = numReducers;
        this.context = context;

        this.collectorExpressions = new ArrayList<>();
        for (Expression expr : parsedStatement.groupByExpressions()) {
            addCollectorExpression(expr);
        }


        aggFunctions = new AggFunction[parsedStatement.aggregateExpressions().size()];
        HashSet<Expression> distinctColumns = new HashSet<>();

        for (int i = 0; i < parsedStatement.aggregateExpressions().size(); i++) {
            AggExpr aggExpr = parsedStatement.aggregateExpressions().get(i);
            aggFunctions[i] = aggFunctionMap.get(aggExpr.functionName);
            addCollectorExpression(aggExpr.expression);
            if (aggExpr.isDistinct) {
                if (!distinctColumns.contains(aggExpr.expression)) {
                    distinctColumns.add(aggExpr.expression);
                }
            }
        }
        if (parsedStatement.hasStoppableAggregate) {
            aggregateHandler = new CheckingSQLGroupingAggregateHandler();
        } else {
            aggregateHandler = new SimpleSQLGroupingAggregateHandler();
        }
        rows = newRows();

    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    public Rows rows() {
        return rows;
    }

    @Override
    public void collect(int doc) throws IOException {
        for (CollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
        GroupByRow row = rows.getRow();
        aggregateHandler.handleAggregates(row, parsedStatement.aggregateExpressions(),
                aggFunctions);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        for (CollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    private void addCollectorExpression(Expression e) {
        if (e instanceof CollectorExpression) {
            CollectorExpression ce = (CollectorExpression) e;
            ce.startCollect(context);
            collectorExpressions.add(ce);
        }
    }

    protected Rows newRows() {
        return new GroupTree(numReducers, parsedStatement,
                context.cacheRecycler());
    }
}
