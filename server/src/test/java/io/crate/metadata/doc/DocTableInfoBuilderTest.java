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

import io.crate.Constants;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collections;
import java.util.Locale;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static io.crate.testing.TestingHelpers.createNodeContext;


public class DocTableInfoBuilderTest extends ESTestCase {

    private NodeContext nodeCtx = createNodeContext();

    private String randomSchema() {
        if (randomBoolean()) {
            return DocSchemaInfo.NAME;
        } else {
            return randomAsciiLettersOfLength(3);
        }
    }

    @Test
    public void testNoTableInfoFromOrphanedPartition() throws Exception {
        String schemaName = randomSchema();
        PartitionName partitionName = new PartitionName(
            new RelationName(schemaName, "test"), Collections.singletonList("boo"));
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(partitionName.asIndexName())
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfReplicas(0)
            .numberOfShards(5)
            .putMapping(Constants.DEFAULT_MAPPING_TYPE,
                "{" +
                "  \"default\": {" +
                "    \"properties\":{" +
                "      \"id\": {" +
                "         \"type\": \"integer\"," +
                "         \"index\": \"not_analyzed\"" +
                "      }" +
                "    }" +
                "  }" +
                "}");
        Metadata metadata = Metadata.builder()
            .put(indexMetadataBuilder)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        DocTableInfoBuilder builder = new DocTableInfoBuilder(
            nodeCtx,
            new RelationName(schemaName, "test"),
            state,
            new IndexNameExpressionResolver()
        );

        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "Relation '%s.test' unknown", schemaName));
        builder.build();
    }
}
