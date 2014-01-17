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

package org.cratedb.index;

import org.cratedb.DataType;

public class ColumnDefinition {
    public final String tableName;
    public final String columnName;
    public final DataType dataType;
    public final String analyzer_method;
    public final boolean dynamic;
    public final boolean strict;
    public final int ordinalPosition;

    /**
     * Create a new ColumnDefinition
     * @param tableName the name of the table this column is in
     * @param columnName the name of the column
     * @param dataType the dataType of the column
     * @param analyzer_method the analyzer_method used on the column
     * @param ordinalPosition the position in the table
     * @param dynamic applies only to objects - if the type of new columns should be mapped,
     *                always false for "normal" columns
     * @param strict applied only to objects - if new columns can be added
     */
    public ColumnDefinition(String tableName, String columnName, DataType dataType, String analyzer_method,
                            int ordinalPosition, boolean dynamic, boolean strict) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.analyzer_method = analyzer_method;
        this.ordinalPosition = ordinalPosition;
        this.dynamic = dynamic;
        this.strict = strict;
    }


    public boolean isMultiValued() {
        // this is not really accurate yet as fields with the plain analyzer method may still contain
        // arrays.
        // but currently it is good enough to throw an exception early in the
        // group by on fields with analyzer case

        // there might also be analyzers which don't generate multi-values and in that case
        // this check is also wrong.
        return analyzer_method != null && !analyzer_method.equals("plain");
    }

    /**
     * returns true if this columnDefinition has a supported DataType
     * @return
     */
    public boolean isSupported() {
        return dataType != DataType.NOT_SUPPORTED;
    }
}
