/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.projection;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.Value;
import io.crate.metadata.RowGranularity;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public abstract class DMLProjection extends Projection {

    private static final List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
        new Value(DataTypes.LONG)  // number of rows updated
    );

    Symbol uidSymbol;

    DMLProjection(Symbol uidSymbol) {
        this.uidSymbol = uidSymbol;
    }

    DMLProjection(StreamInput in) throws IOException {
        uidSymbol = Symbols.fromStream(in);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return OUTPUTS;
    }

    public Symbol uidSymbol() {
        return uidSymbol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DMLProjection that = (DMLProjection) o;
        return Objects.equal(uidSymbol, that.uidSymbol);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), uidSymbol);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(uidSymbol, out);
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.SHARD;
    }

    @Override
    public void replaceSymbols(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        uidSymbol = replaceFunction.apply(uidSymbol);
    }
}
