/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.core.collections.Maps;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataTypes;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;

public final class ConstraintsValidator {

    public static void validate(Object value, Reference targetColumn, Collection<ColumnIdent> notNullColumns) {
        assert targetColumn != null: "targetColumn is required to be able to validate it";
        // Validate NOT NULL constraint
        if (value == null && !targetColumn.isNullable()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot insert null value for column '%s'", targetColumn.ident().columnIdent()));
        }
        validateNotNullOnChildren(value, targetColumn, notNullColumns);
    }

    @SuppressWarnings("unchecked")
    private static void validateNotNullOnChildren(Object value,
                                                  Reference targetColumn,
                                                  Collection<ColumnIdent> notNullColumns) {
        if (targetColumn.valueType() == DataTypes.OBJECT) {
            Map<String, Object> valueMap = (Map<String, Object>) value;
            for (ColumnIdent columnIdent : notNullColumns) {
                if (columnIdent.isChildOf(targetColumn.ident().columnIdent())) {
                    Map<String, Object> map = valueMap;
                    for (String path : columnIdent.path()) {
                        Object nested = Maps.getNested(map, path);
                        if (nested == null) {
                            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                             "Cannot insert null value for column '%s'", columnIdent));
                        }
                        if (nested instanceof Map) {
                            map = (Map<String, Object>) nested;
                        }
                    }
                }
            }
        }
    }

    /**
     * Called to validate constraints for insert statements.
     * We need to validate constraints for all table columns that even if they are not
     * part of the insert statement.
     * <p>
     * example:
     * <pre>
     *     create table test (a int, b int, c int not null);
     *     insert into table (a, b) values (1, 2);
     * </pre>
     *
     * @param notUsedNonGeneratedColumns Non-Generated Columns of the target table that are not used in insert statement
     * @param tableInfo                  The target table info
     */
    public static void validateConstraintsForNotUsedColumns(Collection<ColumnIdent> notUsedNonGeneratedColumns,
                                                            DocTableInfo tableInfo) {
        // Validate NOT NULL constraint
        for (ColumnIdent column : notUsedNonGeneratedColumns) {
            if (!tableInfo.getReference(column).isNullable()) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot insert null value for column '%s'", column));
            }
        }
    }
}
