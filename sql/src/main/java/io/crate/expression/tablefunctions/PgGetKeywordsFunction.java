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
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.sql.Identifiers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public final class PgGetKeywordsFunction extends TableFunctionImplementation<List<Object>> {

    public static final String NAME = "pg_get_keywords";
    private static final FunctionName FUNCTION_NAME = new FunctionName(PgCatalogSchemaInfo.NAME, NAME);
    private static final PgGetKeywordsFunction INSTANCE = new PgGetKeywordsFunction();
    private final FunctionInfo info;
    private final ObjectType returnType;

    public static void register(TableFunctionModule module) {
        module.register(
            FUNCTION_NAME,
            new BaseFunctionResolver(FuncParams.NONE) {

                @Override
                public FunctionImplementation getForTypes(List<DataType> argTypes) throws IllegalArgumentException {
                    assert argTypes.isEmpty() : "argument types for pg_get_keywords must be empty due to FuncParams definition";
                    return PgGetKeywordsFunction.INSTANCE;
                }
            }
        );
    }

    public PgGetKeywordsFunction() {
        returnType = ObjectType.builder()
            .setInnerType("word", DataTypes.STRING)
            .setInnerType("catcode", DataTypes.STRING)
            .setInnerType("catdesc", DataTypes.STRING)
            .build();
        info = new FunctionInfo(
            new FunctionIdent(FUNCTION_NAME, List.of()),
            returnType,
            FunctionInfo.Type.TABLE
        );
    }

    @Override
    public Iterable<Row> evaluate(TransactionContext txnCtx, Input<List<Object>>... args) {
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
    public ObjectType returnType() {
        return returnType;
    }
}
