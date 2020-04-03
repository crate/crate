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

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;

import static io.crate.metadata.functions.Signature.scalar;

public class CurrentSettingFunction extends Scalar<String, Object> {

    private static final String NAME = "current_setting";
    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                FQN,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new CurrentSettingFunction(
                    new FunctionInfo(new FunctionIdent(FQN, argumentTypes), argumentTypes.get(0)),
                    signature
                )
        );

        module.register(
            scalar(
                FQN,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.BOOLEAN.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, argumentTypes) ->
                new CurrentSettingFunction(
                    new FunctionInfo(new FunctionIdent(FQN, argumentTypes), argumentTypes.get(0)),
                    signature
                )
        );
    }

    private final FunctionInfo info;
    private final Signature signature;

    CurrentSettingFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input<Object>... args) {
        assert args.length == 1 || args.length == 2 : "number of args must be 1 or 2";

        final String settingName = (String) args[0].value();
        if (settingName == null) {
            return null;
        }

        final Boolean missingOk = (args.length == 2)
            ? (Boolean) args[1].value()
            : Boolean.FALSE;
        if (missingOk == null) {
            return null;
        }

        final SessionSetting<?> sessionSetting = SessionSettingRegistry.SETTINGS.get(settingName);
        if (sessionSetting == null) {
            if (missingOk) {
                return null;
            } else {
                throw new IllegalArgumentException("Unrecognised Setting: " + settingName);
            }
        }
        return sessionSetting.getValue(txnCtx.sessionSettings());
    }
}
