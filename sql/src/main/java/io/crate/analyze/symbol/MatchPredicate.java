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

import io.crate.analyze.MatchOptionsAnalysis;
import io.crate.analyze.relations.FieldResolver;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatchPredicate extends Symbol {

    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            throw new UnsupportedOperationException("Streaming a MatchPredicate is not supported");
        }
    };

    private final Map<Field, Symbol> identBoostMap;
    private final DataType columnType;
    private final Symbol queryTerm;
    private final String matchType;
    private final Map<String, Symbol> options;

    public MatchPredicate(Map<Field, Symbol> identBoostMap,
                          DataType columnType,
                          Symbol queryTerm,
                          String matchType,
                          Map<String, Symbol> options) {
        this.identBoostMap = identBoostMap;
        this.columnType = columnType;
        this.queryTerm = queryTerm;
        this.matchType = matchType;
        this.options = options;
    }

    public Map<Field, Symbol> identBoostMap() {
        return identBoostMap;
    }

    public Symbol queryTerm() {
        return queryTerm;
    }

    public String matchType() {
        return matchType;
    }

    public Map<String, Symbol> options() {
        return options;
    }

    public DataType columnType() {
        return columnType;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.MATCH_PREDICATE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitMatchPredicate(this, context);
    }

    @Override
    public DataType valueType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Cannot stream MatchPredicate");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream MatchPredicate");
    }

    private Symbol getOrDefault(Map<String, Symbol> map, String key, Symbol defaultValue) {
        Symbol symbol = map.get(key);
        if (symbol == null) {
            return defaultValue;
        }
        return symbol;
    }

    public Symbol tryConvertToFunction(FieldResolver fieldResolver) {
        List<Symbol> args = new ArrayList<>();
        args.add(queryTerm);
        args.add(Literal.of(matchType));
        args.add(getOrDefault(options, "analyzer", Literal.NULL));
        args.add(getOrDefault(options, "boost", Literal.NULL));
        args.add(getOrDefault(options, "cutoff_frequency", Literal.NULL));
        args.add(getOrDefault(options, "fuzziness", Literal.NULL));
        args.add(getOrDefault(options, "fuzzy_rewrite", Literal.NULL));
        args.add(getOrDefault(options, "max_expansions", Literal.NULL));
        args.add(getOrDefault(options, "minimum_should_match", Literal.NULL));
        args.add(getOrDefault(options, "operator", Literal.NULL));
        args.add(getOrDefault(options, "prefix_length", Literal.NULL));
        args.add(getOrDefault(options, "rewrite", Literal.NULL));
        args.add(getOrDefault(options, "slop", Literal.NULL));
        args.add(getOrDefault(options, "tie_breaker", Literal.NULL));
        args.add(getOrDefault(options, "zero_terms_query", Literal.NULL));

        for (Map.Entry<Field, Symbol> entry : identBoostMap.entrySet()) {
            Symbol resolved = fieldResolver.resolveField(entry.getKey());
            if (resolved instanceof Reference) {
                args.add(resolved);
                args.add(entry.getValue());
            } else {
                return this;
            }
        }
        return io.crate.operation.predicate.MatchPredicate.createSymbol(args);
    }

    private static void addOption(Map<String, Object> options, String key, Symbol value) {
        if (value == Literal.NULL) {
            return;
        }
        Object val = ValueSymbolVisitor.VALUE.process(value);
        if (val == null) {
            return;
        }
        if (val instanceof BytesRef) {
            val = ((BytesRef) val).utf8ToString();
        }
        options.put(key, val);
    }

    public static Map<String, Object> extractOptions(List<Symbol> arguments) {
        Map<String, Object> options = new HashMap<>() ;
        addOption(options, "analyzer", arguments.get(2));
        addOption(options, "boost", arguments.get(3));
        addOption(options, "cutoff_frequency", arguments.get(4));
        addOption(options, "fuzziness", arguments.get(5));
        addOption(options, "fuzzy_rewrite", arguments.get(6));
        addOption(options, "max_expansions", arguments.get(7));
        addOption(options, "minimum_should_match", arguments.get(8));
        addOption(options, "operator", arguments.get(9));
        addOption(options, "prefix_length", arguments.get(10));
        addOption(options, "rewrite", arguments.get(11));
        addOption(options, "slop", arguments.get(12));
        addOption(options, "tie_breaker", arguments.get(13));
        addOption(options, "zero_terms_query", arguments.get(14));
        MatchOptionsAnalysis.validate(options);
        return options;
    }

    /**
     * Return the single field of the argument of a match function
     * @throws IllegalArgumentException if the function contains multiple fields
     */
    public static String extractField(List<Symbol> arguments) {
        if (arguments.size() > 17) {
            throw new IllegalArgumentException("More than 1 field found");
        }
        return ((Reference) arguments.get(15)).ident().columnIdent().fqn();
    }

    public static Map<String, Object> extractFields(List<Symbol> arguments) {
        Map<String, Object> fields = new HashMap<>();
        for (int i = 15; i < arguments.size() - 1; i += 2) {
            Object boost = ValueSymbolVisitor.VALUE.process(arguments.get(i + 1));
            String fieldName = ((Reference) arguments.get(i)).ident().columnIdent().fqn();
            fields.put(fieldName, boost);
        }
        return fields;
    }
}
