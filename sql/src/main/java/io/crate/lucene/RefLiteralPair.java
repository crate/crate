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

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;

class RefLiteralPair {

    private final Reference reference;
    private final Input input;

    RefLiteralPair(Function function) {
        assert function.arguments().size() == 2 : "function requires 2 arguments";
        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left instanceof Reference) {
            reference = (Reference) left;
        } else if (right instanceof Reference) {
            reference = (Reference) right;
        } else {
            reference = null;
        }

        if (left.symbolType().isValueSymbol()) {
            input = (Input) left;
        } else if (right.symbolType().isValueSymbol()) {
            input = (Input) right;
        } else {
            input = null;
        }
    }

    boolean isValid() {
        return input != null && reference != null;
    }

    Reference reference() {
        return reference;
    }

    Input input() {
        return input;
    }
}
