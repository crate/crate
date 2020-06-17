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

package io.crate.expression.tablefunctions;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.sql.Identifiers;
import io.crate.types.RowType;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static io.crate.types.DataTypes.STRING;

public final class PgGetKeywordsFunction extends TableFunctionImplementation<List<Object>> {

    private static final String NAME = "pg_get_keywords";
    private static final FunctionName FUNCTION_NAME = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);
    private static final RowType RETURN_TYPE = new RowType(
        List.of(STRING, STRING, STRING),
        List.of("word", "catcode", "catdesc")
    );

    public static void register(TableFunctionModule module) {

        module.register(
            Signature.table(
                FUNCTION_NAME, RETURN_TYPE.getTypeSignature()),
            (signature, argTypes) -> new PgGetKeywordsFunction(
                signature,
                new FunctionInfo(
                    new FunctionIdent(FUNCTION_NAME, argTypes),
                    RETURN_TYPE,
                    FunctionInfo.Type.TABLE
                )
            )
        );
    }

    private final Signature signature;
    private final FunctionInfo info;

    public PgGetKeywordsFunction(Signature signature, FunctionInfo info) {
        this.signature = signature;
        this.info = info;
    }

    @Override
    public Iterable<Row> evaluate(TransactionContext txnCtx, Input<List<Object>>[] args) {
        return () -> Identifiers.KEYWORDS.stream()
            .map(new Function<Identifiers.Keyword, Row>() {

                final Object[] columns = new Object[3];
                final RowN row = new RowN(columns);

                @Override
                public Row apply(Identifiers.Keyword keyword) {
                    columns[0] = keyword.getWord().toLowerCase(Locale.ENGLISH);
                    if (keyword.isReserved()) {
                        columns[1] = "R";
                        columns[2] = "reserved";
                    } else {
                        columns[1] = "U";
                        columns[2] = "unreserved";
                    }
                    return row;
                }
            }).iterator();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public RowType returnType() {
        return RETURN_TYPE;
    }

    @Override
    public boolean hasLazyResultSet() {
        return false;
    }
}
