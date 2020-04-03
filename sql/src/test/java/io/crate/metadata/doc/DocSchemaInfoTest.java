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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.data.Input;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetaData;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.Index;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.script.ScriptException;

import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;
import static io.crate.testing.TestingHelpers.getFunctions;

public class DocSchemaInfoTest extends CrateDummyClusterServiceUnitTest {

    private DocSchemaInfo docSchemaInfo;
    private Functions functions;
    private UserDefinedFunctionService udfService;

    @Before
    public void setup() throws Exception {
        functions = getFunctions();
        udfService = new UserDefinedFunctionService(clusterService, functions);
        udfService.registerLanguage(new UDFLanguage() {
            @Override
            public Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData,
                                                       Signature signature) throws ScriptException {
                String error = validate(metaData);
                if (error != null) {
                    throw new ScriptException("this is not Burlesque");
                }
                return new Scalar() {
                    private final FunctionInfo info = new FunctionInfo(new FunctionIdent(metaData.schema(), metaData.name(), metaData.argumentTypes()), metaData.returnType());
                    @Override
                    public Object evaluate(TransactionContext txnCtx, Input[] args) {
                        return null;
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
                };
            }

            @Override
            @Nullable
            public String validate(UserDefinedFunctionMetaData metadata) {
                if (!metadata.definition().equals("\"Hello, World!\"Q")) {
                    return "this is not Burlesque";
                }
                return null;
            }

            @Override
            public String name() {
                return "burlesque";
            }
        });
        docSchemaInfo = new DocSchemaInfo("doc", clusterService, functions, udfService,
            (ident, state) -> null, new TestingDocTableInfoFactory(ImmutableMap.of()));
    }

    @Test
    public void testInvalidFunction() throws Exception {
        UserDefinedFunctionMetaData invalid = new UserDefinedFunctionMetaData(
            "my_schema", "invalid", ImmutableList.of(), DataTypes.INTEGER,
            "burlesque", "this is not valid burlesque code"
        );
        UserDefinedFunctionMetaData valid = new UserDefinedFunctionMetaData(
            "my_schema", "valid", ImmutableList.of(), DataTypes.INTEGER,
            "burlesque", "\"Hello, World!\"Q"
        );
        UserDefinedFunctionsMetaData metaData = UserDefinedFunctionsMetaData.of(invalid, valid);
        // if a functionImpl can't be created, it won't be registered

        udfService.updateImplementations("my_schema", metaData.functionsMetaData().stream());

        assertThat(functions.get("my_schema", "valid", ImmutableList.of(), pathWithPGCatalogAndDoc()), Matchers.notNullValue());

        expectedException.expectMessage("unknown function: my_schema.invalid()");
        functions.get("my_schema", "invalid", ImmutableList.of(), pathWithPGCatalogAndDoc());
    }

    @Test
    public void testNoNPEIfDeletedIndicesNotInPreviousClusterState() throws Exception {
        // sometimes on startup it occurs that a ClusterChangedEvent contains deleted indices
        // which are not in the previousState.
        MetaData metaData = new MetaData.Builder().build();
        docSchemaInfo.invalidateFromIndex(new Index("my_index", "asdf"), metaData);
    }

}
