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

package io.crate.metadata.doc;

import io.crate.data.Input;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.script.ScriptException;

import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;

public class DocSchemaInfoTest extends CrateDummyClusterServiceUnitTest {

    private DocSchemaInfo docSchemaInfo;
    private UserDefinedFunctionService udfService;
    private NodeContext nodeCtx;

    @Before
    public void setup() throws Exception {
        nodeCtx = createNodeContext();
        udfService = new UserDefinedFunctionService(clusterService, nodeCtx);
        udfService.registerLanguage(new UDFLanguage() {
            @Override
            public Scalar createFunctionImplementation(UserDefinedFunctionMetadata metadata,
                                                       Signature signature) throws ScriptException {
                String error = validate(metadata);
                if (error != null) {
                    throw new ScriptException("this is not Burlesque");
                }
                return new Scalar<>() {
                    @Override
                    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
                        return null;
                    }

                    @Override
                    public Signature signature() {
                        return signature;
                    }

                    @Override
                    public Signature boundSignature() {
                        return signature;
                    }
                };
            }

            @Override
            @Nullable
            public String validate(UserDefinedFunctionMetadata metadata) {
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
        docSchemaInfo = new DocSchemaInfo("doc", clusterService, nodeCtx, udfService,
            (ident, state) -> null, new TestingDocTableInfoFactory(Map.of()));
    }

    @Test
    public void testInvalidFunction() throws Exception {
        UserDefinedFunctionMetadata invalid = new UserDefinedFunctionMetadata(
            "my_schema", "invalid", List.of(), DataTypes.INTEGER,
            "burlesque", "this is not valid burlesque code"
        );
        UserDefinedFunctionMetadata valid = new UserDefinedFunctionMetadata(
            "my_schema", "valid", List.of(), DataTypes.INTEGER,
            "burlesque", "\"Hello, World!\"Q"
        );
        UserDefinedFunctionsMetadata metadata = UserDefinedFunctionsMetadata.of(invalid, valid);
        // if a functionImpl can't be created, it won't be registered

        udfService.updateImplementations("my_schema", metadata.functionsMetadata().stream());

        assertThat(nodeCtx.functions().get("my_schema", "valid", List.of(), pathWithPGCatalogAndDoc()), Matchers.notNullValue());

        expectedException.expectMessage("Unknown function: my_schema.invalid()");
        nodeCtx.functions().get("my_schema", "invalid", List.of(), pathWithPGCatalogAndDoc());
    }

    @Test
    public void testNoNPEIfDeletedIndicesNotInPreviousClusterState() throws Exception {
        // sometimes on startup it occurs that a ClusterChangedEvent contains deleted indices
        // which are not in the previousState.
        Metadata metadata = new Metadata.Builder().build();
        docSchemaInfo.invalidateFromIndex(new Index("my_index", "asdf"), metadata);
    }

}
