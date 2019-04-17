/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.testing;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.expression.scalar.Scalar;
import io.crate.types.DataTypes;

import java.util.Collections;


public class SleepScalarFunction extends Scalar<Boolean, Long> {

    public static final String NAME = "sleep";

    private final static FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(NAME, Collections.singletonList(DataTypes.LONG)),
        DataTypes.BOOLEAN,
        FunctionInfo.Type.SCALAR, FunctionInfo.NO_FEATURES);

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @SafeVarargs
    @Override
    public final Boolean evaluate(TransactionContext txnCtx, Input<Long>... args) {
        long millis = (args.length > 0) ? args[0].value() : 1000L;
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

}

