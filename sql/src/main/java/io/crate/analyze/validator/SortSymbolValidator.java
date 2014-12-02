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

package io.crate.analyze.validator;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

/**
 * validate that sortSymbols don't contain partition by columns
 */
public class SortSymbolValidator {

    private final static InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol, @Nullable List<ColumnIdent> partitionedByColumns) throws UnsupportedOperationException {
        INNER_VALIDATOR.process(symbol, new SortContext(partitionedByColumns));
    }

    static class SortContext {

        private final List<ColumnIdent> partitionedByColumns;
        private boolean inFunction;

        public SortContext(List<ColumnIdent> partitionedByColumns) {
            this.partitionedByColumns = MoreObjects.firstNonNull(partitionedByColumns, ImmutableList.<ColumnIdent>of());
            this.inFunction = false;
        }
    }

    private static class InnerValidator extends SymbolVisitor<SortContext, Void> {

        @Override
        public Void visitFunction(Function symbol, SortContext context) {
            if (!context.inFunction  && !DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH,
                                "Cannot ORDER BY '%s': invalid return type '%s'.",
                                SymbolFormatter.format(symbol),
                                symbol.valueType())
                );
            }
            if (symbol.info().type() == FunctionInfo.Type.PREDICATE) {
                throw new UnsupportedOperationException(String.format(
                        "%s predicate cannot be used in an ORDER BY clause", symbol.info().ident().name()));
            }

            try {
                context.inFunction = true;
                for (Symbol arg : symbol.arguments()) {
                    process(arg, context);
                }
            } finally {
                context.inFunction = false;
            }
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, SortContext context) {
            if (context.partitionedByColumns.contains(symbol.info().ident().columnIdent())) {
                throw new UnsupportedOperationException(
                        SymbolFormatter.format(
                                "cannot use partitioned column %s in ORDER BY clause",
                                symbol));
            }

            // if we are in a function, we do not need to check the data type.
            // the function will do that for us.
            if (!context.inFunction && !DataTypes.PRIMITIVE_TYPES.contains(symbol.info().type())) {
                throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH,
                                "Cannot ORDER BY '%s': invalid data type '%s'.",
                                SymbolFormatter.format(symbol),
                                symbol.valueType())
                );
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new UnsupportedOperationException(
                        String.format("Cannot ORDER BY '%s': sorting on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
            }
            return null;
        }

        @Override
        public Void visitField(Field field, SortContext context) {
            return process(field.target(), context);
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, SortContext context) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot order by \"%s\". The column doesn't exist.", symbol));
        }

        @Override
        public Void visitSymbol(Symbol symbol, SortContext context) {
            return null;
        }
    }
}