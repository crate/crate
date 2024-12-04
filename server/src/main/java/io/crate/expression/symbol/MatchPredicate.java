/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.symbol;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.format.Style;
import io.crate.types.BooleanType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class MatchPredicate implements Symbol {

    private final Map<Symbol, Symbol> identBoostMap;
    private final Symbol queryTerm;
    private final String matchType;
    private final Symbol options;

    public MatchPredicate(Map<Symbol, Symbol> identBoostMap,
                          Symbol queryTerm,
                          String matchType,
                          Symbol options) {
        assert options.valueType().id() == ObjectType.ID : "options symbol must be of type object";
        this.identBoostMap = identBoostMap;
        this.queryTerm = queryTerm;
        this.matchType = matchType;
        this.options = options;
    }

    public Map<Symbol, Symbol> identBoostMap() {
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
    public boolean any(Predicate<? super Symbol> predicate) {
        if (predicate.test(this) || queryTerm.any(predicate) || options.any(predicate)) {
            return true;
        }
        for (var entry : identBoostMap.entrySet()) {
            if (entry.getKey().any(predicate)) {
                return true;
            }
            if (entry.getValue().any(predicate)) {
                return true;
            }
        }
        return false;
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
    public BooleanType valueType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream MatchPredicate");
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0L;
        for (var entry : identBoostMap.entrySet()) {
            bytes += entry.getKey().ramBytesUsed();
            bytes += entry.getValue().ramBytesUsed();
        }
        return bytes
            + queryTerm.ramBytesUsed()
            + RamUsageEstimator.sizeOf(matchType)
            + options.ramBytesUsed();
    }

    @Override
    public String toString(Style style) {
        StringBuilder sb = new StringBuilder("MATCH((");
        Iterator<Map.Entry<Symbol, Symbol>> it = identBoostMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Symbol, Symbol> entry = it.next();
            sb.append(entry.getKey().toString(style));
            sb.append(" ");
            sb.append(entry.getValue().toString(style));
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append("), ");
        sb.append(queryTerm.toString(style));
        sb.append(") USING ");
        sb.append(matchType);
        sb.append(" WITH (");
        sb.append(options.toString(style));
        sb.append(")");
        return sb.toString();
    }
}
