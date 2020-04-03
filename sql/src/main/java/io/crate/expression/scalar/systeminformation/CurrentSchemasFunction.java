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

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CurrentSchemasFunction extends Scalar<List<String>, Boolean> {

    public static final String NAME = "current_schemas";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(FQN, ImmutableList.of(DataTypes.BOOLEAN)),
        DataTypes.STRING_ARRAY,
        FunctionInfo.Type.SCALAR,
        Collections.emptySet());

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                FQN,
                DataTypes.BOOLEAN.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) -> new CurrentSchemasFunction(signature)
        );
    }

    private final Signature signature;

    public CurrentSchemasFunction(Signature signature) {
        this.signature = signature;
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @SafeVarargs
    @Override
    public final List<String> evaluate(TransactionContext txnCtx, Input<Boolean>... args) {
        assert args.length == 1 : "expecting 1 boolean argument";

        Boolean includeImplicitSchemas = args[0].value();
        if (includeImplicitSchemas == null) {
            includeImplicitSchemas = false;
        }

        ArrayList<String> schemas = new ArrayList<>();
        for (String schema : txnCtx.sessionSettings().searchPath()) {
            if (includeImplicitSchemas == false && schema.equals(PgCatalogSchemaInfo.NAME)) {
                continue;
            }
            schemas.add(schema);
        }
        return schemas;
    }
}
