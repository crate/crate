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

package io.crate.expression.operator;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class OrOperator extends Operator<Boolean> {

    public static final String NAME = "op_or";

    public static final Signature SIGNATURE = Signature.scalar(
        NAME,
        DataTypes.BOOLEAN.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature()
    );


    public static void register(OperatorModule module) {
        module.register(
            SIGNATURE,
            OrOperator::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    public OrOperator(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert function != null : "function must not be null";
        assert function.arguments().size() == 2 : "number of args must be 2";

        Symbol left = function.arguments().get(0);
        Symbol right = function.arguments().get(1);

        if (left.symbolType().isValueSymbol() && right.symbolType().isValueSymbol()) {
            return Literal.of(evaluate(txnCtx, nodeCtx, (Input) left, (Input) right));
        }

        /*
         * true  or x    -> true
         * false or x    -> x
         * null  or x    -> null or true -> return function as is
         */
        if (left instanceof Input) {
            Object value = ((Input) left).value();
            if (value == null) {
                return function;
            }
            assert value instanceof Boolean : "value must be Boolean";
            if ((Boolean) value) {
                return Literal.of(true);
            } else {
                return right;
            }
        }

        if (right instanceof Input) {
            Object value = ((Input) right).value();
            if (value == null) {
                return function;
            }
            assert value instanceof Boolean : "value must be Boolean";
            if ((Boolean) value) {
                return Literal.of(true);
            } else {
                return left;
            }
        }

        return function;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Boolean>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

        // implement three valued logic.
        // don't touch anything unless you have a good reason for it! :)
        // http://en.wikipedia.org/wiki/Three-valued_logic
        Boolean left = args[0].value();
        Boolean right = args[1].value();

        if (left == null && right == null) {
            return null;
        }

        if (left == null) {
            return (right) ? true : null;
        }

        if (right == null) {
            return (left) ? true : null;
        }

        return left || right;
    }

}
