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

package io.crate.analyze;

import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.ParameterExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ParamTypeHints implements Function<ParameterExpression, Symbol> {

    public static final ParamTypeHints EMPTY = new ParamTypeHints(Collections.emptyList());

    private final List<DataType> types;

    public ParamTypeHints(List<DataType> types) {
        this.types = types;
    }

    /**
     * Get the type for the parameter at position {@code index}.
     *
     * If the typeHints don't contain a type for the given index it will return Undefined
     * and it may become defined at a later point in time during analysis.
     */
    public DataType getType(int index) {
        if (index + 1 > types.size()) {
            return DataTypes.UNDEFINED;
        }
        return types.get(index);
    }

    @Nullable
    @Override
    public Symbol apply(@Nullable ParameterExpression input) {
        if (input == null) {
            return null;
        }
        return new ParameterSymbol(input.index(), getType(input.index()));
    }

    @Override
    public String toString() {
        return "ParamTypeHints{" + types + '}';
    }
}
