/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.table;

import io.crate.sql.tree.ColumnPolicy;
import io.crate.common.Booleans;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import static io.crate.sql.tree.ColumnPolicy.DYNAMIC;
import static io.crate.sql.tree.ColumnPolicy.IGNORED;
import static io.crate.sql.tree.ColumnPolicy.STRICT;

public final class ColumnPolicies {

    public static final String ES_MAPPING_NAME = "dynamic";
    public static final String CRATE_NAME = "column_policy";

    public static ColumnPolicy decodeMappingValue(Object value) {
        if (value == null) {
            return DYNAMIC;
        }
        return decodeMappingValue(String.valueOf(value));
    }

    public static ColumnPolicy decodeMappingValue(String value) {
        if (Booleans.isTrue(value)) {
            return DYNAMIC;
        }
        if (Booleans.isFalse(value)) {
            return IGNORED;
        }
        if (value.equalsIgnoreCase("strict")) {
            return STRICT;
        }
        throw new IllegalArgumentException("Invalid column policy: " + value);
    }

    public static String encodeMappingValue(ColumnPolicy columnPolicy) {
        switch (columnPolicy) {
            case DYNAMIC:
                return String.valueOf(true);
            case STRICT:
                return "strict";
            case IGNORED:
                return String.valueOf(false);
            default:
                throw new AssertionError("Illegal columnPolicy: " + columnPolicy);
        }
    }

    public static ColumnPolicy of(DataType<?> dataType) {
        if (ArrayType.unnest(dataType) instanceof ObjectType objectType) {
            return objectType.columnPolicy();
        }
        return DYNAMIC;
    }
}
