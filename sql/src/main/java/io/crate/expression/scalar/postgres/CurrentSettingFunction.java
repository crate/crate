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

package io.crate.expression.scalar.postgres;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.Locale;


public class CurrentSettingFunction extends Scalar<String, String> {

    private static final String NAME = "current_setting";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(FQN, ImmutableList.of(DataTypes.STRING)),
        DataTypes.STRING,
        FunctionInfo.Type.SCALAR,
        Collections.emptySet());

    public static void register(ScalarFunctionModule scalarFunctionModule) {
        scalarFunctionModule.register(new CurrentSettingFunction());
    }

    @Override
    public FunctionInfo info() {
        return INFO;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input<String>... args) {
        // todo: change this to include optional boolean arg
        assert args.length == 1 : "number of args must be 1";
        String settingName = args[0].value();
        String settingValue = txnCtx.sessionSettings().sessionSettings().get(settingName);
        if (settingValue == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Unrecognised Setting: %s", settingName));
        }
        return settingValue;
    }
}
