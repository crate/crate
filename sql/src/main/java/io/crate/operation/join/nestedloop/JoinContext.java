/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.join.nestedloop;

import io.crate.executor.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * contains state needed during join execution
 * and must be portable between multiple execution steps
 */
class JoinContext implements Closeable {
    private final NestedLoopOperation.RowCombinator rowCombinator;
    private final int innerPageSize;
    private final int outerPageSize;
    private final AtomicBoolean inFirstIteration;

    private PageInfo innerPageInfo;
    private Page innerPage;
    Iterator<Object[]> innerPageIterator;

    private PageInfo outerPageInfo;
    private Page outerPage;
    Iterator<Object[]> outerPageIterator;
    private Object[] outerRow;

    TaskResult innerTaskResult;
    TaskResult outerTaskResult;


    public JoinContext(TaskResult outerTaskResult,
                       PageInfo outerPageInfo,
                       TaskResult innerTaskResult,
                       PageInfo innerPageInfo,
                       NestedLoopOperation.RowCombinator rowCombinator) {
        this.outerTaskResult = FetchedRowsPageableTaskResult.wrap(outerTaskResult, outerPageInfo);
        this.outerPageInfo = outerPageInfo;
        this.outerPageSize = outerPageInfo.size();
        this.outerPage = this.outerTaskResult.page();

        this.innerTaskResult = FetchedRowsPageableTaskResult.wrap(innerTaskResult, innerPageInfo);
        this.innerPageInfo = innerPageInfo;
        this.innerPageSize = innerPageInfo.size();
        this.innerPage = this.innerTaskResult.page();

        this.rowCombinator = rowCombinator;
        this.inFirstIteration = new AtomicBoolean(true);
    }

    public boolean hasZeroRowRelation() {
        return this.outerTaskResult.page().size() == 0 || this.innerTaskResult.page().size() == 0;
    }

    protected Object[] combine(Object[] outer, Object[] inner) {
        return rowCombinator.combine(outer, inner);
    }

    public PageInfo resetInnerPageInfo() {
        innerPageInfo = PageInfo.firstPage(innerPageSize);
        return innerPageInfo;
    }

    public PageInfo advanceInnerPageInfo() {
        innerPageInfo = innerPageInfo.nextPage();
        return innerPageInfo;
    }

    public PageInfo advanceOuterPageInfo() {
        outerPageInfo = outerPageInfo.nextPage();
        return outerPageInfo;
    }

    @Override
    public void close() throws IOException {
        outerTaskResult.close();
        innerTaskResult.close();
    }

    public synchronized void newInnerPage(TaskResult taskResult) {
        innerTaskResult = taskResult;
        innerPage = taskResult.page();
        innerPageIterator = innerPage.iterator();
    }

    public synchronized void newOuterPage(TaskResult taskResult) {
        outerTaskResult = taskResult;
        outerPage = taskResult.page();
        outerPageIterator = outerPage.iterator();
    }

    public boolean innerIsFinished() {
        return (innerTaskResult != null && (innerTaskResult.page().size() == 0 || innerTaskResult.page().size() < innerPageInfo.size()))
                && (innerPageIterator != null && !innerPageIterator.hasNext());
    }

    public boolean outerIsFinished() {
        return (outerTaskResult != null && (outerTaskResult.page().size() == 0 || outerTaskResult.page().size() < outerPageInfo.size()))
                && (outerPageIterator != null && !outerPageIterator.hasNext());
    }

    public boolean advanceOuterRow() {
        if (outerPageIterator == null || !outerPageIterator.hasNext()) {
            return false;
        }
        outerRow = outerPageIterator.next();
        return true;
    }

    public Object[] outerRow() {
        return outerRow;
    }

    public boolean inFirstIteration() {
        return inFirstIteration.get();
    }

    public void finishedFirstIteration() {
        this.inFirstIteration.set(false);
    }
}
