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
import com.google.common.base.Optional;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.StringValueSymbolVisitor;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WhereClause implements Streamable {

    public static final WhereClause MATCH_ALL = new WhereClause();
    public static final WhereClause NO_MATCH = new WhereClause(null, true);

    private Symbol query;
    private boolean noMatch = false;

    protected String clusteredBy;
    protected Long version;

    protected List<String> partitions = new ArrayList<>();

    private WhereClause() {
    }

    public WhereClause(StreamInput in) throws IOException {
        readFrom(in);
    }

    public WhereClause(Function query) {
        this.query = query;
    }

    public WhereClause(Function query, boolean noMatch) {
        this(query);
        this.noMatch = noMatch;
    }

    public WhereClause(Symbol normalizedQuery) {
        if (normalizedQuery.symbolType().isValueSymbol()) {
            noMatch = !canMatch(normalizedQuery);
        } else {
            query = normalizedQuery;
        }
    }

    public static boolean canMatch(Symbol query) {
        if (query.symbolType().isValueSymbol()) {
            Object value = ((Input) query).value();
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else {
                throw new RuntimeException("Symbol normalized to an invalid value");
            }
        }
        return true;
    }

    public WhereClause normalize(EvaluatingNormalizer normalizer) {
        if (noMatch || query == null) {
            return this;
        }
        Symbol normalizedQuery = normalizer.normalize(query);
        if (normalizedQuery == query) {
            return this;
        }
        WhereClause normalizedWhereClause = new WhereClause(normalizedQuery);
        normalizedWhereClause.partitions = partitions;
        normalizedWhereClause.version = version;
        normalizedWhereClause.clusteredBy = clusteredBy;
        return normalizedWhereClause;
    }


    public Optional<String> clusteredBy() {
        return Optional.fromNullable(clusteredBy);
    }

    public void clusteredByLiteral(@Nullable Literal clusteredByLiteral) {
        assert this != NO_MATCH && this != MATCH_ALL: "may not set clusteredByLiteral on MATCH_ALL/NO_MATCH singleton";
        if (clusteredByLiteral != null) {
            clusteredBy = StringValueSymbolVisitor.INSTANCE.process(clusteredByLiteral);
        }
    }

    public void version(@Nullable Long version) {
        assert this != NO_MATCH && this != MATCH_ALL: "may not set version on MATCH_ALL/NO_MATCH singleton";
        this.version = version;
    }

    public Optional<Long> version() {
        return Optional.fromNullable(this.version);
    }

    public void partitions(List<Literal> partitions) {
        assert this != NO_MATCH && this != MATCH_ALL: "may not set partitions on MATCH_ALL/NO_MATCH singleton";
        for (Literal partition : partitions) {
            this.partitions.add(StringValueSymbolVisitor.INSTANCE.process(partition));
        }
    }

    /**
     * Returns a predefined list of partitions this query can be executed on
     * instead of the entire partition set.
     *
     * If the list is empty no prefiltering can be done.
     *
     * Note that the NO_MATCH case has to be tested separately.
     *
     */
    public List<String> partitions() {
        return partitions;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            query = Symbol.fromStream(in);
        } else {
            noMatch = in.readBoolean();
        }

        if (in.readBoolean()) {
            clusteredBy = in.readBytesRef().utf8ToString();
        }

        if (in.readBoolean()) {
            version = in.readVLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (query != null) {
            out.writeBoolean(true);
            Symbol.toStream(query, out);
        } else {
            out.writeBoolean(false);
            out.writeBoolean(noMatch);
        }

        if (clusteredBy != null) {
            out.writeBoolean(true);
            out.writeBytesRef(new BytesRef(clusteredBy));
        } else {
            out.writeBoolean(false);
        }

        if (version != null) {
            out.writeBoolean(true);
            out.writeVLong(version);
        } else {
            out.writeBoolean(false);
        }
    }

    public boolean hasQuery() {
        return query != null;
    }

    @Nullable
    public Symbol query() {
        return query;
    }

    public boolean noMatch() {
        return noMatch;
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

        if (noMatch != that.noMatch) return false;
        if (clusteredBy != null ? !clusteredBy.equals(that.clusteredBy) : that.clusteredBy != null)
            return false;
        if (partitions != null ? !partitions.equals(that.partitions) : that.partitions != null)
            return false;
        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (noMatch ? 1 : 0);
        result = 31 * result + (clusteredBy != null ? clusteredBy.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
        return result;
    }
}
