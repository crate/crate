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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class SelectAnalyzedStatement extends AbstractDataAnalyzedStatement {

    private Integer limit;
    private int offset = 0;
    private List<Symbol> groupBy;
    protected boolean selectFromFieldCache = false;

    private Symbol havingClause;

    private Multimap<String, Symbol> aliasMap = ArrayListMultimap.create();
    private OrderBy orderBy;

    public SelectAnalyzedStatement(ReferenceInfos referenceInfos, Functions functions,
                                   Analyzer.ParameterContext parameterContext, ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameterContext, referenceResolver);
    }

    public void limit(Integer limit) {
        this.limit = limit;
    }

    public Integer limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public boolean isLimited() {
        return limit != null || offset > 0;
    }

    public void groupBy(List<Symbol> groupBy) {
        if (groupBy != null && groupBy.size() > 0) {
            sysExpressionsAllowed = true;
        }
        this.groupBy = groupBy;
    }

    @Nullable
    public List<Symbol> groupBy() {
        return groupBy;
    }

    public boolean hasGroupBy() {
        return groupBy != null && groupBy.size() > 0;
    }

    @Nullable
    public Symbol havingClause() {
        return havingClause;
    }

    public void havingClause(Symbol clause) {
        this.havingClause = normalizer.process(clause, null);
    }

    @Override
    public boolean hasNoResult() {
        if (havingClause != null && havingClause.symbolType() == SymbolType.LITERAL) {
            Literal havingLiteral = (Literal)havingClause;
            if (havingLiteral.value() == false) {
                return true;
            }
        }

        if (globalAggregate()) {
            return firstNonNull(limit(), 1) < 1 || offset() > 0;
        }
        return noMatch() || (limit() != null && limit() == 0);
    }

    private boolean globalAggregate() {
        return hasAggregates() && !hasGroupBy();
    }

    public boolean hasAggregates() {
        return hasAggregates;
    }

    public void offset(int offset) {
        this.offset = offset;
    }

    public void addAlias(String alias, Symbol symbol) {
        outputNames().add(alias);
        aliasMap.put(alias, symbol);
    }

    @Nullable
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
        if (!sysExpressionsAllowed && hasSysExpressions) {
            throw new UnsupportedOperationException("Selecting system columns from regular " +
                    "tables is currently only supported by queries using group-by or " +
                    "global aggregates.");
        }

        super.normalize();
        normalizer.normalizeInplace(groupBy());
        orderBy.normalize(normalizer);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitSelectStatement(this, context);
    }

    public void orderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public OrderBy orderBy() {
        return orderBy;
    }
}
