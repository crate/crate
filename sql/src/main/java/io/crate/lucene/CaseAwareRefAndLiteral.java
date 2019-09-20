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
import io.crate.metadata.Reference;

import javax.annotation.Nullable;
import java.util.List;

final class CaseAwareRefAndLiteral {

    private final Reference ref;
    private final Literal literal;
    private final boolean ignoreCase;

    @Nullable
    public static CaseAwareRefAndLiteral of(Function function) {
        List<Symbol> args = function.arguments();
        assert args.size() == 3 : "Function must have 3 arguments";

        Symbol fst = args.get(0);
        Symbol snd = args.get(1);
        Symbol trd = args.get(2);
        if (fst instanceof Reference && snd instanceof Literal && trd instanceof Literal) {
            boolean ignoreCase = ((Literal<Boolean>) trd).value();
            return new CaseAwareRefAndLiteral((Reference) fst, (Literal) snd, ignoreCase);
        } else {
            return null;
        }
    }

    private CaseAwareRefAndLiteral(Reference ref, Literal literal, boolean ignoreCase) {
        this.ref = ref;
        this.literal = literal;
        this.ignoreCase = ignoreCase;
    }

    Reference reference() {
        return ref;
    }

    Literal literal() {
        return literal;
    }

    boolean ignoreCase() {
        return ignoreCase;
    }
}
