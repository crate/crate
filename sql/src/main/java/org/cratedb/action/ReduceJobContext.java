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

package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

public class ReduceJobContext extends PlainListenableActionFuture<SQLReduceJobResponse> {

    public final GroupByRowComparator comparator;
    public final Object lock = new Object();
    public final ParsedStatement parsedStatement;

    private Rows reducedRows;
    private final AtomicInteger failures = new AtomicInteger(0);

    private ReduceJobRequestContext reduceJobRequestContext;
    private UUID contextId;

    AtomicInteger shardsToProcess;

    public ReduceJobContext(ParsedStatement parsedStatement, ThreadPool threadPool, int shardsToProcess) {
        this(parsedStatement, threadPool, shardsToProcess, null, null);
    }

    public ReduceJobContext(ParsedStatement parsedStatement,
                            ThreadPool threadPool,
                            int shardsToProcess,
                            @Nullable UUID contextId,
                            @Nullable ReduceJobRequestContext reduceJobRequestContext) {
        super(false, threadPool);
        this.parsedStatement = parsedStatement;
        this.shardsToProcess = new AtomicInteger(shardsToProcess);
        this.contextId = contextId;
        this.reduceJobRequestContext = reduceJobRequestContext;
        this.comparator = new GroupByRowComparator(
            GroupByHelper.buildFieldExtractor(parsedStatement, null),
            parsedStatement.orderByIndices()
        );
    }

    public Collection<GroupByRow> terminate(){
        final List<GroupByRow> rowList = new ArrayList<>();
        if (reducedRows != null) {
            reducedRows.walk(new Rows.RowVisitor() {
                @Override
                public void visit(GroupByRow row) {
                    if (parsedStatement.reducerHasRowAuthority()) {
                        row.terminatePartial();
                    }
                    rowList.add(row);
                }
            });
        }
        return GroupByHelper.trimRows(rowList, comparator, parsedStatement.totalLimit());
    }

    public void merge(SQLGroupByResult groupByResult) {
        synchronized (lock) {
            if (reducedRows==null){
                reducedRows = groupByResult.rows();
            } else {
                reducedRows.merge(groupByResult.rows());
            }
        }

        countDown();
    }

    private void countDown() {
        if (shardsToProcess.decrementAndGet() == 0) {
            if (reduceJobRequestContext != null) {
                reduceJobRequestContext.remove(contextId);
            }
            set(new SQLReduceJobResponse(terminate(), parsedStatement));
        }
    }

    public void timeout() {
        if (!isDone()) {
            setException(new SQLReduceJobTimeoutException());
        }
    }

    public void countFailure() {
        failures.incrementAndGet();
        countDown();
    }

    public int failures() {
        return failures.get();
    }
}
