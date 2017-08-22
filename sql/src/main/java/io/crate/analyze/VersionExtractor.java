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

package io.crate.analyze;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.FunctionCopyVisitor;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.Operators;

import java.util.ArrayList;
import java.util.List;

/**
 * Class which can be used to rewrite a function tree so that all `_version = X` occurrences are replaced with `true`.
 *
 * The replaced version values are kept in {@link Result#versions}.
 */
public final class VersionExtractor {

    private static final VersionExtractorAndReplacerVisitor VISITOR = new VersionExtractorAndReplacerVisitor();

    public static Result extractVersionComparisons(Symbol query) {
        ArrayList<Symbol> versions = new ArrayList<>();
        Symbol newQuery = VISITOR.process(query, versions);
        return new Result(newQuery, versions);
    }

    public static class Result {

        public final Symbol newQuery;
        public final List<Symbol> versions;

        Result(Symbol newQuery, List<Symbol> versions) {
            this.newQuery = newQuery;
            this.versions = versions;
        }
    }

    private static class VersionExtractorAndReplacerVisitor extends FunctionCopyVisitor<ArrayList<Symbol>> {

        @Override
        public Symbol visitFunction(Function function, ArrayList<Symbol> versions) {
            String functionName = function.info().ident().name();
            if (Operators.LOGICAL_OPERATORS.contains(functionName)) {
                function = processAndMaybeCopy(function, versions);
                return function;
            }
            if (functionName.equals(EqOperator.NAME)) {
                assert function.arguments().size() == 2 : "function's number of arguments must be 2";
                Symbol left = function.arguments().get(0);
                if (left instanceof Reference && ((Reference) left).ident().columnIdent().equals(DocSysColumns.VERSION)) {
                    versions.add(function.arguments().get(1));
                    return Literal.BOOLEAN_TRUE;
                }
            }
            return function;
        }
    }
}
