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

package io.crate.exceptions;

import org.elasticsearch.rest.RestStatus;

public class ColumnUnknownException extends CrateException {
    private final String tableName;
    private final String columnName;

    public ColumnUnknownException(String columnName) {
        super(String.format("Column '%s' unknown", columnName));
        this.tableName = null;
        this.columnName = columnName;
    }

    public ColumnUnknownException(String columnName, Throwable e) {
        super(String.format("Column '%s' unknown", columnName), e);
        this.tableName = null;
        this.columnName = columnName;
    }

    public ColumnUnknownException(String tableName, String columnName) {
        super(String.format("Column '%s' unknown", columnName));
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public ColumnUnknownException(String tableName, String columnName, Throwable e) {
        super(String.format("Column '%s' unknown", columnName), e);
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public int errorCode() {
        return 4043;
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public Object[] args() {
        return new Object[]{tableName, columnName};
    }
}
