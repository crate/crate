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

package io.crate.expression.scalar.systeminformation;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

public final class ObjDescriptionFunction extends Scalar<String, Object> {

    private static final String NAME = "obj_description";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);
    private final FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(FQN, new BaseFunctionResolver(FuncParams.builder(Param.NUMERIC, Param.STRING).build()) {
            @Override
            public FunctionImplementation getForTypes(List<DataType> types) throws IllegalArgumentException {
                return new ObjDescriptionFunction(types);
            }
        });
    }

    public ObjDescriptionFunction(List<DataType> argumentTypes) {
        info = new FunctionInfo(
            new FunctionIdent(FQN, argumentTypes),
            DataTypes.STRING
        );
    }

    @SafeVarargs
    @Override
    public final String evaluate(TransactionContext txnCtx, Input<Object>... args) {
        // CrateDB doesn't support comments for database objects, so always return null
        return null;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
