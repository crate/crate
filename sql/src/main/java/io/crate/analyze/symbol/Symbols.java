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

package io.crate.analyze.symbol;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.crate.Streamer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationSplitter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReferenceInfo;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.*;

public class Symbols {

    private static final HasColumnVisitor HAS_COLUMN_VISITOR = new HasColumnVisitor();

    public static final com.google.common.base.Function<Symbol, DataType> TYPES_FUNCTION =
            new com.google.common.base.Function<Symbol, DataType>() {
                @Nullable
                @Override
                public DataType apply(@Nullable Symbol input) {
                    assert input != null : "can't convert null symbol to dataType";
                    return input.valueType();
                }
            };

    public static final Predicate<Symbol> IS_GENERATED_COLUMN = new Predicate<Symbol>() {
        @Override
        public boolean apply(@Nullable Symbol input) {
            return input instanceof Reference && ((Reference)input).info() instanceof GeneratedReferenceInfo;
        }
    };

    public static List<DataType> extractTypes(List<? extends Symbol> symbols) {
        return Lists.transform(symbols, TYPES_FUNCTION);
    }

    public static Streamer<?>[] streamerArray(Collection<? extends Symbol> symbols) {
        Streamer<?>[] streamers = new Streamer<?>[symbols.size()];
        Iterator<? extends Symbol> iter = symbols.iterator();
        for (int i = 0; i < symbols.size(); i++) {
            streamers[i] = iter.next().valueType().streamer();
        }
        return streamers;
    }


    /**
     * returns true if the symbol contains the given columnIdent.
     * If symbol is a Function the function tree will be traversed
     */
    public static boolean containsColumn(@Nullable Symbol symbol, ColumnIdent columnIdent) {
        if (symbol == null) {
            return false;
        }
        return HAS_COLUMN_VISITOR.process(symbol, columnIdent);
    }

    /**
     * returns true if any of the symbols contains the given column
     */
    public static boolean containsColumn(Iterable<? extends Symbol> symbols, ColumnIdent columnIdent) {
        for (Symbol symbol : symbols) {
            if (containsColumn(symbol, columnIdent)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsRelation(Symbol symbol, AnalyzedRelation relation) {
        Set<AnalyzedRelation> relations = new HashSet<>();
        RelationSplitter.RelationCounter.INSTANCE.process(symbol, relations);
        return relations.contains(relation);
    }

    private static class HasColumnVisitor extends SymbolVisitor<ColumnIdent, Boolean> {

        @Override
        protected Boolean visitSymbol(Symbol symbol, ColumnIdent context) {
            return false;
        }

        @Override
        public Boolean visitFunction(Function symbol, ColumnIdent context) {
            for (Symbol arg : symbol.arguments()) {
                if (process(arg, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitFetchReference(FetchReference fetchReference, ColumnIdent context) {
            if (process(fetchReference.docId(), context)) {
                return true;
            }
            return process(fetchReference.ref(), context);
        }

        @Override
        public Boolean visitReference(Reference symbol, ColumnIdent context) {
            return context.equals(symbol.ident().columnIdent());
        }
    }
}
