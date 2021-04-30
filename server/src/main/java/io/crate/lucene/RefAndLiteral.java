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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;

import javax.annotation.Nullable;
import java.util.List;

final class RefAndLiteral {

    private final Reference ref;
    private final Literal literal;

    @Nullable
    public static RefAndLiteral of(Function function) {
        List<Symbol> args = function.arguments();
        assert args.size() == 2 : "Function must have 2 arguments";

        Symbol fst = args.get(0);
        Symbol snd = args.get(1);
        if (fst instanceof Reference && snd instanceof Literal) {
            return new RefAndLiteral((Reference) fst, (Literal) snd);
        } else {
            return null;
        }
    }

    private RefAndLiteral(Reference ref, Literal literal) {
        this.ref = ref;
        this.literal = literal;
    }

    Reference reference() {
        return ref;
    }

    Literal literal() {
        return literal;
    }
}
