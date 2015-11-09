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

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

public class MatchPredicate extends Symbol {

    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            throw new UnsupportedOperationException("Streaming a MatchPredicate is not supported");
        }
    };

    private final Map<Field, Double> identBoostMap;
    private final DataType columnType;
    private final Object queryTerm;
    private final String matchType;
    private final Map<String, Object> options;

    public MatchPredicate(Map<Field, Double> identBoostMap,
                          DataType columnType,
                          Object queryTerm,
                          String matchType,
                          Map<String, Object> options) {
        this.identBoostMap = identBoostMap;
        this.columnType = columnType;
        this.queryTerm = queryTerm;
        this.matchType = matchType;
        this.options = options;
    }

    public Map<Field, Double> identBoostMap() {
        return identBoostMap;
    }

    public Object queryTerm() {
        return queryTerm;
    }

    public String matchType() {
        return matchType;
    }

    public Map<String, Object> options() {
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
}
