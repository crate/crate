/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;

import java.util.Arrays;

public abstract class AbstractScalarFunctionsTest extends CrateUnitTest {
    protected SqlExpressions sqlExpressions;
    protected Functions functions;

    @SuppressWarnings("unchecked")
    protected <T extends FunctionImplementation> T getFunction(String functionName, DataType... argTypes) {
        return (T) functions.get(new FunctionIdent(functionName, Arrays.asList(argTypes)));
    }

    @Before
    public void prepareFunctions() throws Exception {
        TableInfo tableInfo = TestingTableInfo.builder(new TableIdent(DocSchemaInfo.NAME, "users"), null)
                .add("id", DataTypes.INTEGER)
                .add("name", DataTypes.STRING)
                .build();
        TableRelation tableRelation = new TableRelation(tableInfo);
        sqlExpressions = new SqlExpressions(ImmutableMap.<QualifiedName, AnalyzedRelation>of(new QualifiedName("users"), tableRelation));
        functions = sqlExpressions.getInstance(Functions.class);
    }
}
