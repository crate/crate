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

package io.crate.index;

import io.crate.DataType;

public class BuiltInColumnDefinition extends ColumnDefinition {
    /**
     * common tableName for BuiltInColumnDefinitions
     */
    public static final String VIRTUAL_SYSTEM_COLUMN_TABLE = "__";

    public static final BuiltInColumnDefinition SCORE_COLUMN =
            new BuiltInColumnDefinition("_score", DataType.DOUBLE, null, -1, false, true);

    /**
     * @param columnName      the name of the column
     * @param dataType        the dataType of the column
     * @param ordinalPosition the position in the table
     * @param dynamic         applies only to objects - if the type of new columns should be mapped,
     *                        always false for "normal" columns
     * @param strict          applied only to objects - if new columns can be added
     */
    private BuiltInColumnDefinition(String columnName, DataType dataType, String analyzer,
                                    int ordinalPosition, boolean dynamic, boolean strict) {
        super(VIRTUAL_SYSTEM_COLUMN_TABLE, columnName, dataType, analyzer, ordinalPosition, dynamic, strict);
    }
}
