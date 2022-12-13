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

package io.crate.expression.scalar.object;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class ObjectDeepMergeFunction extends Scalar<Map<String, Object>, Map<String, Object>> {

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                "object_deepmerge",
                DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                DataTypes.UNTYPED_OBJECT.getTypeSignature()
            ),
            ObjectDeepMergeFunction::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public ObjectDeepMergeFunction(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @SafeVarargs
    @Override
    public final Map<String, Object> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Map<String, Object>>... args) {
        var objectOne = args[0].value();
        var objectTwo = args[1].value();
        if (objectOne == null) {
            return objectTwo;
        }
        if (objectTwo == null) {
            return objectOne;
        }
        Map<String,Object> resultObject = new HashMap<>();
        resultObject.putAll(objectOne);
        deepMerge(resultObject,objectTwo);
        return resultObject;
    }

    private void deepMerge(Map<String, Object> objectOne, Map<String, Object> objectTwo) {
        for (String key : objectTwo.keySet()) {
            if (objectOne.get(key) instanceof Map && objectTwo.get(key) instanceof Map) {
                deepMerge((Map)objectOne.get(key), (Map)objectTwo.get(key));
            } else {
                objectOne.put(key, objectTwo.get(key));
            }
        }
    }
}
