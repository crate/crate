/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;

public class MatchFunction implements FunctionImplementation<Function> {

    public static final String NAME = "match";

    public static void register(ScalarFunctionModule module) {
        for (DataType dataType : ImmutableList.of(DataType.NULL, DataType.STRING)) {
            FunctionIdent functionIdent = new FunctionIdent(MatchFunction.NAME, ImmutableList.of(dataType, DataType.STRING));
            module.registerScalarFunction(new MatchFunction(new FunctionInfo(functionIdent, DataType.BOOLEAN)));
        }
    }

    private final FunctionInfo info;

    @Inject
    public MatchFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        return function;
    }
}
