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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * This scalar function removes the 3-valued logic from the tree of operators below it.
 * If used as a normal scalar (eg. SELECT ignore3vl(<some_boolean_expression)) it just
 * evaluates NULL to false.
 *
 * Its main usage though is in the WHERE clause because it acts as a marker
 * that {@link io.crate.lucene.LuceneQueryBuilder} can use in order to skip the 3-valued logic
 * filtering on queries which results in a better performance. The 3-valued logic filtering is
 * applied with a generic function filtering which is slow, so if this logic is not needed and
 * the null can be translated to false, the generic function is completely removed.
 */
public class Ignore3vlFunction extends Scalar<Boolean, Boolean> {

    public static final String NAME = "ignore3vl";

    private static final FunctionInfo FUNCTION_INFO = new FunctionInfo(
        new FunctionIdent(NAME, Collections.singletonList(DataTypes.BOOLEAN)), DataTypes.BOOLEAN);

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.BOOLEAN.getTypeSignature(),
                DataTypes.BOOLEAN.getTypeSignature()
            ),
            (signature, argumentTypes) -> new Ignore3vlFunction(signature)
        );
    }

    private final Signature signature;

    public Ignore3vlFunction(Signature signature) {
        this.signature = signature;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<Boolean>... args) {
        assert args.length == 1 : "ignore3vl expects exactly 1 argument, got: " + args.length;
        Boolean value = args[0].value();
        if (value == null) {
            return Boolean.FALSE;
        }
        return value;
    }

    @Override
    public FunctionInfo info() {
        return FUNCTION_INFO;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }
}
