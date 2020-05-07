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

package io.crate.expression.udf;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.FuncResolver;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static java.util.stream.Collectors.toList;


public class UserDefinedFunctionsTest extends UdfUnitTest {

    private Functions functions;
    private SqlExpressions sqlExpressions;

    private Map<FunctionName, List<FuncResolver>> functionImplementations = new HashMap<>();

    @Before
    public void prepare() throws Exception {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        DocTableInfo users = sqlExecutor.schemas().getTableInfo(new RelationName("doc", "users"));
        sqlExpressions = new SqlExpressions(Map.of(users.ident(), new DocTableRelation(users)));
        functions = sqlExpressions.functions();
        udfService.registerLanguage(DUMMY_LANG);
    }

    private void registerUserDefinedFunction(
        String lang,
        String schema,
        String name,
        DataType returnType,
        List<DataType> types,
        String definition) {

        var udf = new UserDefinedFunctionMetaData(
            schema,
            name,
            types.stream().map(FunctionArgumentDefinition::of).collect(toList()),
            returnType,
            lang,
            definition);

        var functionName = new FunctionName(udf.schema(), udf.name());
        var resolvers = functionImplementations.computeIfAbsent(
            functionName, k -> new ArrayList<>());
        resolvers.add(udfService.buildFunctionResolver(udf));
        functions.registerUdfFunctionImplementationsForSchema(
            schema,
            functionImplementations);
    }

    @Test
    public void testOverloadingBuiltinFunctions() {
        registerUserDefinedFunction(
            DUMMY_LANG.name(),
            "test",
            "subtract",
            DataTypes.INTEGER,
            List.of(DataTypes.INTEGER, DataTypes.INTEGER),
            "function subtract(a, b) { return a + b; }");
        assertThat(
            sqlExpressions.asSymbol("test.subtract(2::integer, 1::integer)"),
            isLiteral(DummyFunction.RESULT));
    }
}
