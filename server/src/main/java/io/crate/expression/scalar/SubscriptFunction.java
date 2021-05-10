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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static io.crate.expression.scalar.SubscriptObjectFunction.tryToInferReturnTypeFromObjectTypeAndArguments;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

/**
 * Supported subscript expressions:
 * <ul>
 *     <li>obj['x']</li>
 *     <li>arr[1]</li>
 *     <li>obj_array[1]</li>
 *     <li>obj_array['x']</li>
 * </ul>
 **/
public class SubscriptFunction extends Scalar<Object, Object[]> {

    public static final String NAME = "subscript";

    public static void register(ScalarFunctionModule module) {
        // All signatures but `array[int]` must forbid coercion.
        // Otherwise they would also match for non-int numeric indices like e.g. `array[long]`

        // subscript(array(object)), text) -> array(undefined)
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(object)"),
                DataTypes.STRING.getTypeSignature(),
                parseTypeSignature("array(undefined)")
            ).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupIntoListObjectsByName
                )
        );
        // subscript(array(any)), integer) -> any
        module.register(
            Signature
                .scalar(
                    NAME,
                    parseTypeSignature("array(E)"),
                    DataTypes.INTEGER.getTypeSignature(),
                    parseTypeSignature("E"))
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupByNumericIndex
                )
        );
        // subscript(object(text, element), text) -> undefined
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNDEFINED.getTypeSignature()
            ).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupByName
                )
        );
        // subscript(undefined, text) -> undefined
        //
        // This signature is required for the cases when the look up
        // path is longer than 1, e.g. {\"x\" = { \"y\" = 'test'}}['x']['y'].
        // The first subscript function cannot infer the return type and always
        // returns the undefined type. Therefore, the second subscript function
        // must be resolved for the `subscript(undefined, text)` signature.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.UNDEFINED.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNDEFINED.getTypeSignature()
            ).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupByName
                )
        );
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final BiFunction<Object, Object, Object> lookup;

    private SubscriptFunction(Signature signature,
                              Signature boundSignature,
                              BiFunction<Object, Object, Object> lookup) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.lookup = lookup;
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
    public Symbol normalizeSymbol(Function func, TransactionContext txnCtx, NodeContext nodeCtx) {
        Symbol result = evaluateIfLiterals(this, txnCtx, nodeCtx, func);
        if (result instanceof Literal) {
            return result;
        }
        if (func.arguments().get(0).valueType().id() == ObjectType.ID) {
            return tryToInferReturnTypeFromObjectTypeAndArguments(func);
        }
        return func;
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 : "invalid number of arguments";
        Object element = args[0].value();
        Object index = args[1].value();
        if (element == null || index == null) {
            return null;
        }
        return lookup.apply(element, index);
    }

    static Object lookupIntoListObjectsByName(Object base, Object name) {
        List<?> values = (List<?>) base;
        List<Object> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            Map<?, ?> map = (Map<?, ?>) values.get(i);
            result.add(map.get(name));
        }
        return result;
    }

    static Object lookupByNumericIndex(Object base, Object index) {
        List<?> values = (List<?>) base;
        int idx = ((Number) index).intValue();
        try {
            return values.get(idx - 1); // SQL array indices start with 1
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    static Object lookupByName(Object base, Object name) {
        if (!(base instanceof Map)) {
            throw new IllegalArgumentException("Base argument to subscript must be an object, not " + base);
        }
        Map<?, ?> map = (Map<?, ?>) base;
        if (!map.containsKey(name)) {
            throw new IllegalArgumentException("The object `" + base + "` does not contain the key `" + name + "`");
        }
        return map.get(name);
    }
}
