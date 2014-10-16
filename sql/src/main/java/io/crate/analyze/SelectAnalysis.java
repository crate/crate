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

package io.crate.analyze;

import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.where.WhereClause;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.relation.AnalyzedQuerySpecification;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class SelectAnalysis extends AbstractDataAnalysis {

    private List<Symbol> groupBy;
    private boolean[] reverseFlags;
    private Boolean[] nullsFirst;
    private List<Symbol> sortSymbols;
    protected boolean selectFromFieldCache = false;

    private AnalyzedQuerySpecification querySpecification;

    private Multimap<String, Symbol> aliasMap = ArrayListMultimap.create();

    public SelectAnalysis(ReferenceInfos referenceInfos, Functions functions,
                          Analyzer.ParameterContext parameterContext, ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameterContext, referenceResolver);
    }

    public AnalyzedQuerySpecification querySpecification() {
        return querySpecification;
    }

    public void querySpecification(AnalyzedQuerySpecification relation) {
        this.querySpecification = relation;
    }

    @Deprecated
    public Integer limit() {
        return querySpecification.limit();
    }

    @Deprecated
    public int offset() {
        return querySpecification.offset();
    }

    public boolean isLimited() {
        return querySpecification.limit() != null || querySpecification.offset() > 0;
    }

    @Deprecated
    public void groupBy(List<Symbol> groupBy) {
        if (groupBy != null && groupBy.size() > 0) {
            sysExpressionsAllowed = true;
        }
        this.groupBy = groupBy;
    }

    @Deprecated
    @Nullable
    public List<Symbol> groupBy() {
        return groupBy;
    }

    public boolean hasGroupBy() {
        return groupBy != null && groupBy.size() > 0;
    }

    public void reverseFlags(boolean[] reverseFlags) {
        this.reverseFlags = reverseFlags;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public void sortSymbols(List<Symbol> sortSymbols) {
        this.sortSymbols = sortSymbols;
    }

    @Override
    public boolean hasNoResult() {
        Symbol havingClause = querySpecification.having().orNull();
        if (havingClause != null && havingClause.symbolType() == SymbolType.LITERAL) {
            Literal havingLiteral = (Literal)havingClause;
            if (havingLiteral.value() == false) {
                return true;
            }
        }

        if (globalAggregate()) {
            return Objects.firstNonNull(querySpecification.limit(), 1) < 1 || querySpecification.offset() > 0;
        }
        Integer limit = querySpecification.limit();
        return querySpecification.whereClause().noMatch() || (limit != null && limit == 0);
    }

    @Override
    public WhereClause whereClause() {
        throw new UnsupportedOperationException("whereClause on SelectAnalysis has been deprecated. Use QuerySpecification.whereClause()");
    }

    private boolean globalAggregate() {
        return hasAggregates() && !hasGroupBy();
    }

    @Deprecated
    @Nullable
    public List<Symbol> sortSymbols() {
        return sortSymbols;
    }

    public boolean isSorted() {
        return sortSymbols != null && sortSymbols.size() > 0;
    }

    public boolean hasAggregates() {
        return hasAggregates;
    }

    public void addAlias(String alias, Symbol symbol) {
        outputNames().add(alias);
        aliasMap.put(alias, symbol);
    }

    public Symbol symbolFromAlias(String alias) {
        Collection<Symbol> symbols = aliasMap.get(alias);
        if (symbols.size() > 1) {
            throw new AmbiguousColumnAliasException(alias);
        }
        if (symbols.isEmpty()) {
            return null;
        }

        return symbols.iterator().next();
    }

    @Override
    public void normalize() {
        if (!sysExpressionsAllowed && hasSysExpressions()) {
            throw new UnsupportedOperationException("Selecting system columns from regular " +
                    "tables is currently only supported by queries using group-by or " +
                    "global aggregates.");
        }
        querySpecification.normalize(normalizer);
        if (onlyScalarsAllowed && querySpecification.whereClause().hasQuery()) {
            for (Function function : allocationContext().allocatedFunctions.keySet()) {
                if (function.info().type() != FunctionInfo.Type.AGGREGATE
                        && !(functions.get(function.info().ident()) instanceof Scalar)) {
                    throw new UnsupportedFeatureException(
                            "function not supported on system tables: " + function.info().ident());
                }
            }
        }
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitSelectAnalysis(this, context);
    }

    public void nullsFirst(Boolean[] nullsFirst) {
        this.nullsFirst = nullsFirst;
    }

    public Boolean[] nullsFirst() {
        return this.nullsFirst;
    }
}
