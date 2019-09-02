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

package io.crate.planner.operators;

import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

final class OperatorUtils {

    private OperatorUtils() {
    }

    /**
     * Return columns which are not used.
     *
     * Examples:
     *
     * <pre>
     * toCollect: [f(x)]        used: x
     * unused:    []
     *
     * toCollect: [x, f(x)]     used: [f(x)]
     * unused:    []
     *
     * toCollect: [x, y]        used: [x]
     * unused:    [y]
     * </pre>
     */
    static List<Symbol> getUnusedColumns(List<Symbol> toCollect, Set<Symbol> usedColumns) {
        ArrayList<Symbol> unusedCols = new ArrayList<>();
        Consumer<Reference> maybeAddUnusedRef = r -> {
            if (!usedColumns.contains(r) && !Symbols.containsColumn(usedColumns, r.column())) {
                unusedCols.add(r);
            }
        };
        Consumer<Field> maybeAddUnusedField = f -> {
            if (!usedColumns.contains(f) && !Symbols.containsColumn(usedColumns, f.path())) {
                unusedCols.add(f);
            }
        };
        for (Symbol symbol : toCollect) {
            if (usedColumns.contains(symbol)) {
                continue;
            }
            RefVisitor.visitRefs(symbol, maybeAddUnusedRef);
            FieldsVisitor.visitFields(symbol, maybeAddUnusedField);
        }
        return unusedCols;
    }
}
