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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.ValueSymbolVisitor;
import io.crate.analyze.where.DocKeys;
import io.crate.metadata.TransactionContext;
import io.crate.operation.operator.AndOperator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class WhereClause extends QueryClause implements Streamable {

    public static final WhereClause MATCH_ALL = new WhereClause(Literal.BOOLEAN_TRUE);
    public static final WhereClause NO_MATCH = new WhereClause(Literal.BOOLEAN_FALSE);

    private Optional<Set<Symbol>> clusteredBy = Optional.empty();

    private Optional<DocKeys> docKeys = Optional.empty();

    private List<String> partitions = new ArrayList<>();


    public WhereClause(StreamInput in) throws IOException {
        readFrom(in);
    }

    public WhereClause(@Nullable Symbol normalizedQuery,
                       @Nullable DocKeys docKeys,
                       @Nullable List<String> partitions) {
        super(normalizedQuery);
        docKeys(docKeys);
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
        if (normalizedQuery == query) {
            return this;
        }
        WhereClause normalizedWhereClause = new WhereClause(normalizedQuery,
            docKeys.orElse(null), partitions);
        normalizedWhereClause.clusteredBy = clusteredBy;
        return normalizedWhereClause;
    }

    public Optional<Set<Symbol>> clusteredBy() {
        return clusteredBy;
    }

    @Nullable
    public Set<String> routingValues() {
        if (clusteredBy.isPresent()) {
            HashSet<String> result = new HashSet<>(clusteredBy.get().size());
            Iterators.addAll(result, Iterators.transform(
                clusteredBy.get().iterator(), ValueSymbolVisitor.STRING.function));
            return result;
        } else {
            return null;
        }
    }

    public void clusteredBy(@Nullable Set<Symbol> clusteredBy) {
        assert this != NO_MATCH && this != MATCH_ALL : "may not set clusteredByLiteral on MATCH_ALL/NO_MATCH singleton";
        if (clusteredBy != null && !clusteredBy.isEmpty()) {
            this.clusteredBy = Optional.of(clusteredBy);
        }
    }

    public void partitions(List<Literal> partitions) {
        assert this != NO_MATCH && this != MATCH_ALL : "may not set partitions on MATCH_ALL/NO_MATCH singleton";
        for (Literal partition : partitions) {
            this.partitions.add(ValueSymbolVisitor.STRING.process(partition));
        }
    }

    public void docKeys(@Nullable DocKeys docKeys) {
        assert this != NO_MATCH && this != MATCH_ALL : "may not set docKeys on MATCH_ALL/NO_MATCH singleton";
        if (docKeys == null) {
            this.docKeys = Optional.empty();
        } else {
            this.docKeys = Optional.of(docKeys);
        }
    }

    public Optional<DocKeys> docKeys() {
        return docKeys;
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
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            query = Symbols.fromStream(in);
        } else {
            noMatch = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (query != null) {
            out.writeBoolean(true);
            Symbols.toStream(query, out);
        } else {
            out.writeBoolean(false);
            out.writeBoolean(noMatch);
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WhereClause)) return false;

        WhereClause that = (WhereClause) o;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (noMatch != that.noMatch) return false;
        if (!docKeys.equals(that.docKeys)) return false;
        if (!clusteredBy.equals(that.clusteredBy)) return false;
        if (partitions != null ? !partitions.equals(that.partitions) : that.partitions != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (noMatch ? 1 : 0);
        result = 31 * result + (clusteredBy != null ? clusteredBy.hashCode() : 0);
        result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
        return result;
    }

    public boolean hasVersions() {
        return docKeys.isPresent() && docKeys.get().withVersions();
    }


    /**
     * Adds another query to this WhereClause.
     * <p>
     * The result is either a new WhereClause or the same (but modified) instance.
     */
    public WhereClause add(Symbol otherQuery) {
        assert !docKeys.isPresent() : "Cannot add otherQuery if there are docKeys in the WhereClause";
        if (this == MATCH_ALL) {
            return new WhereClause(otherQuery);
        }
        if (this == NO_MATCH) {
            // NO_MATCH & anything is still NO_MATCH
            return NO_MATCH;
        }
        this.query = AndOperator.of(this.query, otherQuery);
        return this;
    }

    WhereClause copyAndReplace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        if (!hasQuery()) {
            return this;
        }
        Symbol newQuery = replaceFunction.apply(query);
        return new WhereClause(newQuery, docKeys.orElse(null), partitions);
    }

    public WhereClause mergeWhere(WhereClause where2) {
        if (noMatch || where2.noMatch()) {
            return WhereClause.NO_MATCH;
        } else if (!hasQuery() || this == WhereClause.MATCH_ALL) {
            return where2;
        } else if (!where2.hasQuery() || where2 == WhereClause.MATCH_ALL) {
            return this;
        }

        return new WhereClause(AndOperator.join(ImmutableList.of(where2.query(), query)));
    }
}
