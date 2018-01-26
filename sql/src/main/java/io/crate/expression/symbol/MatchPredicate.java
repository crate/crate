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

package io.crate.expression.symbol;

import io.crate.planner.ExplainLeaf;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class MatchPredicate extends Symbol {

    private final Map<Field, Symbol> identBoostMap;
    private final Symbol queryTerm;
    private final String matchType;
    private final Symbol options;

    public MatchPredicate(Map<Field, Symbol> identBoostMap,
                          Symbol queryTerm,
                          String matchType,
                          Symbol options) {
        assert options.valueType().equals(DataTypes.OBJECT) : "options symbol must be of type object";
        this.identBoostMap = identBoostMap;
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

    public Symbol options() {
        return options;
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
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream MatchPredicate");
    }

    @Override
    public String representation() {
        return "MATCH{" + identBoostMap.keySet()
            .stream()
            .map(ExplainLeaf::representation)
            .collect(Collectors.joining(", ")) + '}';
    }
}
