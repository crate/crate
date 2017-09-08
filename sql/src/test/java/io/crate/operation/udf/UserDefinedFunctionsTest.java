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

package io.crate.operation.udf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.sql.tree.QualifiedName;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.testing.SymbolMatchers.isLiteral;


public class UserDefinedFunctionsTest extends UdfUnitTest {

    private Functions functions;
    private SqlExpressions sqlExpressions = new SqlExpressions(
        ImmutableMap.of(new QualifiedName("users"), new DocTableRelation(TableDefinitions.USER_TABLE_INFO))
    );

    private Map<FunctionIdent, FunctionImplementation> functionImplementations = new HashMap<>();

    @Before
    public void prepare() throws Exception {
        functions = sqlExpressions.functions();
        udfService.registerLanguage(DUMMY_LANG);
    }

    private void registerUserDefinedFunction(String lang, String schema, String name, DataType returnType, List<DataType> types, String definition) throws ScriptException {
        UserDefinedFunctionMetaData udfMeta = new UserDefinedFunctionMetaData(
            schema,
            name,
            types.stream().map(FunctionArgumentDefinition::of).collect(Collectors.toList()),
            returnType,
            lang,
            definition
        );

        functionImplementations.put(
            new FunctionIdent(schema, name, types),
            udfService.getLanguage(lang).createFunctionImplementation(udfMeta)
        );
        functions.registerUdfResolversForSchema(schema, functionImplementations);
    }

    @Test
    public void testOverloadingBuiltinFunctions() throws Exception {
        registerUserDefinedFunction(DUMMY_LANG.name(), "test", "subtract", DataTypes.INTEGER, ImmutableList.of(DataTypes.INTEGER, DataTypes.INTEGER), "function subtract(a, b) { return a + b; }");
        String expr = "test.subtract(2::integer, 1::integer)";
        assertThat(sqlExpressions.asSymbol(expr), isLiteral(DummyFunction.RESULT));
    }
}
