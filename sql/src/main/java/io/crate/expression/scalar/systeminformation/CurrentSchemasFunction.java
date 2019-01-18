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
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.format.FunctionFormatSpec;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;

public class CurrentSchemasFunction extends Scalar<String[], Object> implements FunctionFormatSpec {

    private static final String NAME = "current_schemas";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(FQN, ImmutableList.of(DataTypes.BOOLEAN)),
        DataTypes.STRING_ARRAY,
        FunctionInfo.Type.SCALAR,
        Collections.emptySet());

    public static void register(ScalarFunctionModule module) {
        module.register(new CurrentSchemasFunction());
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public String[] evaluate(TransactionContext txnCtx, Input<Object>... args) {
        assert args.length == 1 : "expecting 1 boolean argument";

        Boolean includeImplicitSchemas = (Boolean) args[0].value();
        if (includeImplicitSchemas == null) {
            includeImplicitSchemas = false;
        }

        ArrayList<String> schemas = new ArrayList<>();
        for (String schema : txnCtx.searchPath()) {
            if (includeImplicitSchemas == false && schema.equals(PgCatalogSchemaInfo.NAME)) {
                continue;
            }
            schemas.add(schema);
        }
        return schemas.toArray(new String[0]);
    }

    @Override
    public String beforeArgs(Function function) {
        return NAME;
    }

    @Override
    public String afterArgs(Function function) {
        return "";
    }

    @Override
    public boolean formatArgs(Function function) {
        return false;
    }
}
