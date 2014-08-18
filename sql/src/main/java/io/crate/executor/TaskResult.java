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

public abstract class TaskResult {

    protected static final Object[][] EMPTY_ROWS = new Object[0][];

    public static final NonQueryResult ZERO = new NonQueryResult(0L);
    public static final NonQueryResult ONE_ROW = new NonQueryResult(1L);
    public static final NonQueryResult ROW_COUNT_UNKNOWN = new NonQueryResult(-1L);

    public static final QueryResult EMPTY_RESULT = new QueryResult(EMPTY_ROWS);

    public TaskResult() {

    }

    public abstract Object[][] rows();

    public abstract long rowCount();
}
