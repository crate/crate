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

import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;
import static io.crate.metadata.doc.DocSchemaInfo.getTablesAffectedByPublicationsChange;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.ScriptException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Scalar;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.table.Operation;
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;

public class DocSchemaInfoTest extends CrateDummyClusterServiceUnitTest {

    private DocSchemaInfo docSchemaInfo;
    private UserDefinedFunctionService udfService;
    private NodeContext nodeCtx;

    @Before
    public void setup() throws Exception {
        nodeCtx = createNodeContext();
        var docTableFactory = new DocTableInfoFactory(nodeCtx);
        udfService = new UserDefinedFunctionService(clusterService, docTableFactory, nodeCtx);
        udfService.registerLanguage(new UDFLanguage() {
            @Override
            public Scalar createFunctionImplementation(UserDefinedFunctionMetadata metadata,
                                                       Signature signature,
                                                       BoundSignature boundSignature) throws ScriptException {
                String error = validate(metadata);
                if (error != null) {
                    throw new ScriptException("this is not Burlesque");
                }
                return new Scalar<>(signature, boundSignature) {
                    @Override
                    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
                        return null;
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
        docSchemaInfo = new DocSchemaInfo(
            "doc",
            clusterService,
            nodeCtx,
            udfService,
            new ViewInfoFactory(() -> null),
            new DocTableInfoFactory(nodeCtx)
        );
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

        assertThat(nodeCtx.functions().get("my_schema", "valid", List.of(), pathWithPGCatalogAndDoc())).isNotNull();

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

    @Test
    public void test_update_when_tables_are_published() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        var publishNewTables = publicationsMetadata("pub1", false, List.of("t1", "t2"));

        assertThat(getTablesAffectedByPublicationsChange(null, publishNewTables, state))
            .containsExactlyInAnyOrder("t1", "t2");
    }

    @Test
    public void test_publish_all_tables() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        var publishAllTables = publicationsMetadata("pub1", true, List.of());

        assertThat(getTablesAffectedByPublicationsChange(null, publishAllTables, state))
            .containsExactlyInAnyOrder("t1", "t2", "t3", "t4");
    }

    @Test
    public void test_remove_all_tables() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        var prevMetadata = publicationsMetadata("pub1", true, List.of());
        var newMetadata = publicationsMetadata("pub1", false, List.of());

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state))
            .containsExactlyInAnyOrder("t1", "t2", "t3", "t4");
    }

    @Test
    public void test_add_and_remove_tables() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        // publish t3, t4 drop t1, t2
        var prevMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));
        var newMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2", "t3", "t4"));

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state))
            .containsExactlyInAnyOrder("t3", "t4");

        // publish t3, t4 drop t2
        prevMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));
        newMetadata = publicationsMetadata("pub1", false, List.of("t1", "t3", "t4"));

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state))
            .containsExactlyInAnyOrder("t2", "t3", "t4");
    }

    @Test
    public void test_publish_all_tables_when_tables_have_been_previously_published() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        // publish t3, t4 drop t1, t2
        var prevMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));
        var newMetadata = publicationsMetadata("pub1", true, List.of());

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state))
            .containsExactlyInAnyOrder("t3", "t4");
    }

    @Test
    public void test_unpublish_all_tables_when_tables_have_been_previously_published() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        var prevMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));
        var newMetadata = publicationsMetadata("pub1", false, List.of());

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state))
            .containsExactlyInAnyOrder("t1", "t2");
    }

    @Test
    public void test_drop_all_previous_published_tables() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        var prevMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));
        var newMetadata = publicationsMetadata("pub1", false, List.of());

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state))
            .containsExactlyInAnyOrder("t1", "t2");
    }

    @Test
    public void test_nothing_changed() throws Exception {
        var state = docTablesByName("t1", "t2", "t3", "t4");
        var prevMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));
        var newMetadata = publicationsMetadata("pub1", false, List.of("t1", "t2"));

        assertThat(getTablesAffectedByPublicationsChange(prevMetadata, newMetadata, state)).isEmpty();
        assertThat(getTablesAffectedByPublicationsChange(null, null, state)).isEmpty();
    }

    private PublicationsMetadata publicationsMetadata(String name, boolean allTables, List<String> tables) {
        var relationNames = Lists2.map(tables, x -> new RelationName(Schemas.DOC_SCHEMA_NAME, x));
        var publications = Map.of(name, new Publication("user1", allTables, relationNames));
        return new PublicationsMetadata(publications);
    }

    private Map<String, DocTableInfo> docTablesByName(String ... tables) {
        var result = new HashMap<String, DocTableInfo>();
        for (String table : tables) {
            result.put(table, docTableInfo(table));
        }
        return result;
    }

    private DocTableInfo docTableInfo(String name) {
        return new DocTableInfo(
            new RelationName(Schemas.DOC_SCHEMA_NAME, name),
            Map.of(),
            Map.of(),
            Map.of(),
            null,
            List.of(),
            List.of(),
            null,
            new String[0],
            new String[0],
            5,
            "0",
            Settings.EMPTY,
            List.of(),
            List.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );
    }
}
