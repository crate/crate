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

import io.crate.data.Input;
import io.crate.exceptions.VersioninigValidationException;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.doc.DocSysColumns;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class WhereClause {

    // Using null here instead of Literal.BOOLEAN_TRUE
    // so that printers can distinguish between explicit `WHERE TRUE` and absence of WHERE
    public static final WhereClause MATCH_ALL = new WhereClause(null);
    public static final WhereClause NO_MATCH = new WhereClause(Literal.BOOLEAN_FALSE);

    public static boolean canMatch(Symbol query) {
        if (query instanceof Input) {
            Object value = ((Input<?>) query).value();
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else {
                throw new IllegalArgumentException("Expected query value to be of type `Boolean`, but got: " + value);
            }
        }
        return true;
    }

    @Nullable
    private final Symbol query;
    private final Set<Symbol> clusteredBy;
    private final List<String> partitions;

    public WhereClause(@Nullable Symbol query) {
        this.query = query;
        this.partitions = List.of();
        this.clusteredBy = Set.of();
    }

    public WhereClause(@Nullable Symbol normalizedQuery,
                       @Nullable List<String> partitions,
                       Set<Symbol> clusteredBy) {
        this.clusteredBy = clusteredBy;
        this.partitions = Objects.requireNonNullElse(partitions, List.of());
        this.query = normalizedQuery;
        if (normalizedQuery != null) {
            validateVersioningColumnsUsage();
        }
    }

    public boolean hasQuery() {
        return query != null;
    }

    @Nullable
    public Symbol query() {
        return query;
    }

    public Symbol queryOrFallback() {
        return query == null ? Literal.BOOLEAN_TRUE : query;
    }

    private void validateVersioningColumnsUsage() {
        if (Symbols.containsColumn(query, DocSysColumns.SEQ_NO)) {
            if (!Symbols.containsColumn(query, DocSysColumns.PRIMARY_TERM)) {
                throw VersioninigValidationException.seqNoAndPrimaryTermUsage();
            } else {
                if (Symbols.containsColumn(query, DocSysColumns.VERSION)) {
                    throw VersioninigValidationException.mixedVersioningMeachanismsUsage();
                }
            }
        } else if (Symbols.containsColumn(query, DocSysColumns.PRIMARY_TERM)) {
            if (!Symbols.containsColumn(query, DocSysColumns.SEQ_NO)) {
                throw VersioninigValidationException.seqNoAndPrimaryTermUsage();
            } else {
                if (Symbols.containsColumn(query, DocSysColumns.VERSION)) {
                    throw VersioninigValidationException.mixedVersioningMeachanismsUsage();
                }
            }
        }
    }

    public Set<Symbol> clusteredBy() {
        return clusteredBy;
    }

    @Nullable
    public Set<String> routingValues() {
        if (clusteredBy.isEmpty() == false) {
            HashSet<String> result = new HashSet<>(clusteredBy.size());
            for (Symbol symbol : clusteredBy) {
                assert symbol instanceof Literal : "clustered by symbols must be literals";
                result.add(((Literal<?>) symbol).value().toString());
            }
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
        return "WhereClause{" +
               "clusteredBy=" + clusteredBy +
               ", partitions=" + partitions +
               ", query=" + query +
               '}';
    }

    public boolean hasVersions() {
        return Symbols.containsColumn(query, DocSysColumns.VERSION);
    }

    public boolean hasSeqNoAndPrimaryTerm() {
        return Symbols.containsColumn(query, DocSysColumns.SEQ_NO) &&
               Symbols.containsColumn(query, DocSysColumns.PRIMARY_TERM);
    }

    /**
     * Returns a new WhereClause that contains the query of this `AND`ed with `otherQuery`.
     */
    public WhereClause add(Symbol otherQuery) {
        if (query == null || query.equals(Literal.BOOLEAN_TRUE)) {
            return new WhereClause(otherQuery, partitions, clusteredBy);
        } else {
            if (canMatch(query)) {
                return new WhereClause(AndOperator.of(query, otherQuery), partitions, clusteredBy);
            } else {
                return this;
            }
        }
    }

    public WhereClause map(Function<? super Symbol, ? extends Symbol> mapper) {
        if (query == null && clusteredBy.isEmpty()) {
            return this;
        }
        Symbol newQuery = query == null ? null : mapper.apply(query);
        HashSet<Symbol> newClusteredBy = new HashSet<>(clusteredBy.size());
        for (Symbol symbol : clusteredBy) {
            newClusteredBy.add(mapper.apply(symbol));
        }
        return new WhereClause(newQuery, partitions, newClusteredBy);
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
