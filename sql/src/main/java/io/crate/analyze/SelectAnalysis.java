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
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Query;
import org.cratedb.sql.AmbiguousAliasException;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class SelectAnalysis extends AbstractDataAnalysis {

    private Query query;

    private Integer limit;
    private int offset = 0;
    private List<Symbol> groupBy;
    private boolean[] reverseFlags;
    private List<Symbol> sortSymbols;

    private Multimap<String, Symbol> aliasMap = ArrayListMultimap.create();

    public Query query() {
        return query;
    }

    public void query(Query query) {
        this.query = query;
    }

    public SelectAnalysis(ReferenceInfos referenceInfos, Functions functions,
                          Object[] parameters, ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameters, referenceResolver);
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

    public void groupBy(List<Symbol> groupBy) {
        this.groupBy = groupBy;
    }

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
        if (globalAggregate()) {
            return Objects.firstNonNull(limit(), 1) < 1 || offset() > 0;
        }
        return noMatch() || (limit() != null && limit() == 0);
    }

    private boolean globalAggregate() {
        return hasAggregates() && !hasGroupBy();
    }

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

    public void offset(int offset) {
        this.offset = offset;
    }

    public void addAlias(String alias, Symbol symbol) {
        outputNames().add(alias);
        aliasMap.put(alias, symbol);
    }

    public Symbol symbolFromAlias(String alias) {
        Collection<Symbol> symbols = aliasMap.get(alias);
        if (symbols.size() > 1) {
            throw new AmbiguousAliasException(alias);
        }
        if (symbols.isEmpty()) {
            return null;
        }

        return symbols.iterator().next();
    }

    @Override
    public void normalize() {
        super.normalize();
        normalizer.normalizeInplace(groupBy());
        normalizer.normalizeInplace(sortSymbols());
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitSelectAnalysis(this, context);
    }
}
