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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.Build;
import org.elasticsearch.Version;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Locale;

public class VersionFunction extends Scalar<String, Void> {

    public static final String NAME = "version";

    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);

    protected static final FunctionInfo INFO = new FunctionInfo(
        new FunctionIdent(FQN, Collections.emptyList()), DataTypes.STRING,
        FunctionInfo.Type.SCALAR, FunctionInfo.NO_FEATURES);

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                FQN,
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) -> new VersionFunction(signature)
        );

    }

    private static String formatVersion() {
        String version = Version.displayVersion(Version.CURRENT, Version.CURRENT.isSnapshot());
        String built = String.format(
            Locale.ENGLISH,
            "built %s/%s",
            Build.CURRENT.hashShort(),
            Build.CURRENT.timestamp());
        String vmVersion = String.format(
            Locale.ENGLISH,
            "%s %s",
            System.getProperty("java.vm.name"),
            System.getProperty("java.vm.version"));
        String osVersion = String.format(
            Locale.ENGLISH,
            "%s %s %s",
            System.getProperty("os.name"),
            System.getProperty("os.version"),
            System.getProperty("os.arch"));

        return String.format(
            Locale.ENGLISH,
            "CrateDB %s (%s, %s, %s)",
            version,
            built,
            osVersion,
            vmVersion);
    }

    private static final String VERSION = formatVersion();

    private final Signature signature;

    public VersionFunction(Signature signature) {
        this.signature = signature;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input<Void>... args) {
        return VERSION;
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
}
