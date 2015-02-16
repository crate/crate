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

package io.crate.executor;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * The result of executing a {@linkplain io.crate.executor.Task}.
 */
public interface TaskResult extends Closeable {

    Object[][] EMPTY_ROWS = new Object[0][];
    RowCountResult ZERO = new RowCountResult(0L);
    RowCountResult ONE_ROW = new RowCountResult(1L);
    RowCountResult ROW_COUNT_UNKNOWN = new RowCountResult(-1L);
    RowCountResult FAILURE = new RowCountResult(-2L);
    QueryResult EMPTY_RESULT = new QueryResult(EMPTY_ROWS);

    Object[][] rows();


    /**
     * get the page identified by <code>pageInfo</code>.
     *
     * @param pageInfo identifying the page to fetch
     * @return a future holding the result of fetching the next page
     */
    ListenableFuture<TaskResult> fetch(PageInfo pageInfo);

    /**
     *
     * Get the page that was fetched with this task result.
     * It might contain less rows than requested.
    * @return a Page you can iterate over to get the rows it consists of
    */
    Page page();

    /**
     * can be set in bulk operations to set the error for a single operation
     */
    @Nullable
    String errorMessage();
}
