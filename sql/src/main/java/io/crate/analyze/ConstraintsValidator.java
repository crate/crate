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

import io.crate.analyze.symbol.Reference;
import io.crate.metadata.GeneratedReferenceInfo;
import io.crate.metadata.ReferenceInfo;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public final class ConstraintsValidator {

    public static void validate(Object value, ReferenceInfo targetColumn) {
        // Validate NOT NULL constraint
        if (value == null && !targetColumn.isNullable()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot insert null value for column %s", targetColumn.ident().columnIdent().fqn()));
        }
    }

    /**
     * Called to validate constraints for insert statements.
     * We need to validate constraints for all table columns that even if they are not
     * part of the insert statement.
     *
     * example:
     * <pre>
     *     create table test (a int, b int, c int not null);
     *     insert into table (a, b) values (1, 2);
     * </pre>
     *
     * @param values The values to be inserted
     * @param targetColumns The target columns of the insert statement
     * @param tableColumns All columns of the target table
     */
    public static void validateNonGeneratedColumns(Object[] values,
                                                   Reference[] targetColumns,
                                                   Iterable<? extends ReferenceInfo> tableColumns) {
        // Validate NOT NULL constraints
        Set<String> columnsWithNonNullValues = new HashSet<>();
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                columnsWithNonNullValues.add(targetColumns[i].ident().columnIdent().fqn());
            }
        }

        for (ReferenceInfo referenceInfo : tableColumns) {
            if (!(referenceInfo instanceof GeneratedReferenceInfo) && !referenceInfo.isNullable()) {
                if (!columnsWithNonNullValues.contains(referenceInfo.ident().columnIdent().fqn())) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Cannot insert null value for column %s", referenceInfo.ident().columnIdent().fqn()));
                }
            }
        }
    }

}
