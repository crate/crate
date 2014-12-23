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

import io.crate.exceptions.Exceptions;

import javax.annotation.Nullable;

public class RowCountResult implements TaskResult {

    @Nullable private final Throwable error;
    private final Object[][] rows;

    public RowCountResult(long rowCount) {
        this(rowCount, null);
    }

    private RowCountResult(long rowCount, Throwable throwable) {
        this.rows = new Object[][] { new Object[] { rowCount }};
        this.error = throwable;
    }

    public static RowCountResult error(Throwable throwable) {
        return new RowCountResult(-2L, throwable);
    }
    
    @Override
    public Object[][] rows() {
        return rows;
    }

    @Nullable
    @Override
    public String errorMessage() {
        if (error == null) {
            return null;
        }
        return Exceptions.messageOf(error);
    }
}
