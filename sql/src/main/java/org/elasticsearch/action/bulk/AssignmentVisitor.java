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

package org.elasticsearch.action.bulk;

import io.crate.analyze.symbol.*;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.*;

public class AssignmentVisitor extends SymbolVisitor<AssignmentVisitor.AssignmentVisitorContext, Symbol> {

    private static final AssignmentVisitor INSTANCE = new AssignmentVisitor();

    public static class AssignmentVisitorContext {

        private List<InputColumn> inputColumns = new ArrayList<>();
        private List<Integer> indices = new ArrayList<>();
        private DataType[] dataTypes;

        public List<Integer> columnIndicesToStream() {
            return indices;
        }

        public DataType[] dataTypes() {
            return dataTypes;
        }
    }

    /**
     * Process update and insert assignments to extract data types and define which row values
     * must be streamed.
     */
    public static AssignmentVisitorContext processAssignments(@Nullable Map<Reference, Symbol> updateAssignments,
                                                              @Nullable Map<Reference, Symbol> insertAssignments) {
        AssignmentVisitorContext context = new AssignmentVisitorContext();
        if (updateAssignments != null) {
            for (Symbol symbol : updateAssignments.values()) {
                INSTANCE.process(symbol, context);
            }
        }
        if (insertAssignments != null) {
            for (Symbol symbol : insertAssignments.values()) {
                INSTANCE.process(symbol, context);
            }
        }

        Collections.sort(context.inputColumns, new Comparator<InputColumn>() {
            @Override
            public int compare(InputColumn o1, InputColumn o2) {
                return Integer.compare(o1.index(), o2.index());
            }
        });

        context.dataTypes = new DataType[context.inputColumns.size()];
        for (int i = 0; i < context.inputColumns.size(); i++) {
            context.dataTypes[i] = context.inputColumns.get(i).valueType();
        }

        return context;
    }

    @Override
    public Symbol visitInputColumn(InputColumn inputColumn, AssignmentVisitorContext context) {
        if (!context.inputColumns.contains(inputColumn)) {
            context.inputColumns.add(inputColumn);
            context.indices.add(inputColumn.index());
        }
        return inputColumn;
    }

    @Override
    public Symbol visitFunction(Function symbol, AssignmentVisitorContext context) {
        for (Symbol argument : symbol.arguments()) {
            process(argument, context);
        }
        return symbol;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, AssignmentVisitorContext context) {
        return symbol;
    }
}
