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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.Reference;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;

abstract class CmpQuery implements FunctionToQuery {

    @Nullable
    protected Tuple<Reference, Literal> prepare(Function input) {
        assert input != null : "function must not be null";
        assert input.arguments().size() == 2 : "function's number of arguments must be 2";

        Symbol left = input.arguments().get(0);
        Symbol right = input.arguments().get(1);

        if (!(left instanceof Reference) || !(right.symbolType().isValueSymbol())) {
            return null;
        }
        assert right.symbolType() == SymbolType.LITERAL :
            "right.symbolType() must be " + SymbolType.LITERAL;
        return new Tuple<>((Reference) left, (Literal) right);
    }
}
