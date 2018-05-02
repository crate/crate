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

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.eval.NullEliminator;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.ValueSymbolVisitor;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class WhereClause extends QueryClause {

    public static final WhereClause MATCH_ALL = new WhereClause(Literal.BOOLEAN_TRUE);
    public static final WhereClause NO_MATCH = new WhereClause(Literal.BOOLEAN_FALSE);

    private Set<Symbol> clusteredBy = Collections.emptySet();
    private List<String> partitions = Collections.emptyList();


    public WhereClause(@Nullable Symbol normalizedQuery,
                       @Nullable List<String> partitions,
                       Set<Symbol> clusteredBy) {
        super(normalizedQuery);
        this.clusteredBy = clusteredBy;
        if (partitions != null) {
            this.partitions = partitions;
        }
    }

    public WhereClause(@Nullable Symbol query) {
        super(query);
    }

    public WhereClause normalize(EvaluatingNormalizer normalizer, TransactionContext transactionContext) {
        if (noMatch || query == null) {
            return this;
        }
        Symbol normalizedQuery = normalizer.normalize(query, transactionContext);
        Symbol nullReplacedQuery = NullEliminator.eliminateNullsIfPossible(
            normalizedQuery, s -> normalizer.normalize(s, transactionContext));
        if (nullReplacedQuery == query) {
            return this;
        }
        return new WhereClause(nullReplacedQuery, partitions, clusteredBy);
    }

    public Set<Symbol> clusteredBy() {
        return clusteredBy;
    }

    @Nullable
    public Set<String> routingValues() {
        if (clusteredBy.isEmpty() == false) {
            HashSet<String> result = new HashSet<>(clusteredBy.size());
            Iterators.addAll(result, Iterators.transform(
                clusteredBy.iterator(), ValueSymbolVisitor.STRING.function));
            return result;
        } else {
            return null;
        }
    }

    /**
     * Returns a predefined list of partitions this query can be executed on
     * instead of the entire partition set.
     * <p>
     * If the list is empty no prefiltering can be done.
     * <p>
     * Note that the NO_MATCH case has to be tested separately.
     */
    public List<String> partitions() {
        return partitions;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        if (noMatch()) {
            helper.add("NO_MATCH", true);
        } else if (!hasQuery()) {
            helper.add("MATCH_ALL", true);
        } else {
            helper.add("query", query);
        }
        return helper.toString();
    }

    public boolean hasVersions() {
        return query != null && Symbols.containsColumn(query, DocSysColumns.VERSION);
    }


    /**
     * Adds another query to this WhereClause.
     * <p>
     * The result is either a new WhereClause or the same (but modified) instance.
     */
    public WhereClause add(Symbol otherQuery) {
        if (this == MATCH_ALL) {
            return new WhereClause(otherQuery);
        }
        if (this == NO_MATCH) {
            // NO_MATCH & anything is still NO_MATCH
            return NO_MATCH;
        }
        if (this.query == null) {
            return new WhereClause(otherQuery);
        }
        this.query = AndOperator.of(this.query, otherQuery);
        return this;
    }

    WhereClause copyAndReplace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        if (!hasQuery()) {
            return this;
        }
        Symbol newQuery = replaceFunction.apply(query);
        return new WhereClause(newQuery, partitions, clusteredBy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WhereClause that = (WhereClause) o;
        return Objects.equals(clusteredBy, that.clusteredBy) &&
               Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusteredBy, partitions);
    }
}
