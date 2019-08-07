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

public final class PgTypeofFunction extends Scalar<String, Object> {

    private static final FunctionName FQNAME = new FunctionName(PgCatalogSchemaInfo.NAME, "pg_typeof");
    private static final FuncParams FUNCTION_PARAMS = FuncParams.builder(Param.ANY).build();

    public static void register(ScalarFunctionModule module) {
        module.register(FQNAME, new BaseFunctionResolver(FUNCTION_PARAMS) {
            @Override
            public FunctionImplementation getForTypes(List<DataType> signatureTypes) throws IllegalArgumentException {
                return new PgTypeofFunction(new FunctionInfo(new FunctionIdent(FQNAME, signatureTypes),
                                                             DataTypes.STRING));
            }
        });
    }

    private final FunctionInfo info;
    private final String type;

    private PgTypeofFunction(FunctionInfo info) {
        this.info = info;
        type = this.info.ident().argumentTypes().get(0).getName();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @SafeVarargs
    @Override
    public final String evaluate(TransactionContext txnCtx, Input<Object>... args) {
        assert args.length == 1 : "typeof expects exactly 1 argument, got: " + args.length;
        return type;
    }
}
