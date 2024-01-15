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

package io.crate.execution.engine.window;

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class RowNumberWindowFunction implements WindowFunction {

    private static final String NAME = "row_number";

    public static void register(WindowFunctionModule module) {
        module.register(
            Signature.window(
                NAME,
                DataTypes.INTEGER.getTypeSignature()
            ),
            RowNumberWindowFunction::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    private RowNumberWindowFunction(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Object execute(int idxInPartition,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          @Nullable Boolean ignoreNulls,
                          Input<?> ... args) {
        if (ignoreNulls != null) {
            throw new IllegalArgumentException("row_number cannot accept RESPECT or IGNORE NULLS flag.");
        }
        return idxInPartition + 1;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }
}
