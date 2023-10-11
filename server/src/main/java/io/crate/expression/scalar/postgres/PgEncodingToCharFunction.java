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

package io.crate.expression.scalar.postgres;

import static io.crate.metadata.functions.Signature.scalar;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

public class PgEncodingToCharFunction extends Scalar<String, Integer> {

    private static final FunctionName FQN = new FunctionName(PgCatalogSchemaInfo.NAME, "pg_encoding_to_char");

    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                FQN,
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            PgEncodingToCharFunction:: new
        );
    }

    public PgEncodingToCharFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @SafeVarargs
    @Override
    public final String evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Integer>... args) {
        var value = args[0].value();
        if (value == null || value < 0 || value >= PgEncodingIdentifiers.values().length) {
            return null;
        }
        return PgEncodingIdentifiers.values()[value].name();
    }

    private enum PgEncodingIdentifiers {
        // See https://github.com/postgres/postgres/blob/master/src/include/mb/pg_wchar.h
        SQL_ASCII,
        EUC_JP,
        EUC_CN,
        EUC_KR,
        EUC_TW,
        EUC_JIS_2004,
        UTF8,
        MULE_INTERNAL,
        LATIN1,
        LATIN2,
        LATIN3,
        LATIN4,
        LATIN5,
        LATIN6,
        LATIN7,
        LATIN8,
        LATIN9,
        LATIN10,
        WIN1256,
        WIN1258,
        WIN866,
        WIN874,
        KOI8R,
        WIN1251,
        WIN1252,
        ISO_8859_5,
        ISO_8859_6,
        ISO_8859_7,
        ISO_8859_8,
        WIN1250,
        WIN1253,
        WIN1254,
        WIN1255,
        WIN1257,
        KOI8U,
        SJIS,
        BIG5,
        GBK,
        UHC,
        GB18030,
        JOHAB,
        SHIFT_JIS_2004;
    }
}
