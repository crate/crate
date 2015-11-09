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

import com.google.common.util.concurrent.MoreExecutors;
import io.crate.Constants;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocTableInfoBuilderTest extends CrateUnitTest {

    private ExecutorService executorService;

    @Before
    public void prepare() throws Exception {
        executorService = MoreExecutors.newDirectExecutorService();
    }

    private String randomSchema() {
        if (randomBoolean()) {
            return DocSchemaInfo.NAME;
        } else {
            return randomAsciiOfLength(3);
        }
    }

    @Test
    public void testNoTableInfoFromOrphanedPartition() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        String schemaName = randomSchema();
        PartitionName partitionName = new PartitionName(schemaName, "test", Collections.singletonList(new BytesRef("boo")));
        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(partitionName.asIndexName())
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
        MetaData metaData = MetaData.builder()
                .put(indexMetaDataBuilder)
                .build();
        when(clusterState.metaData()).thenReturn(metaData);

        DocTableInfoBuilder builder = new DocTableInfoBuilder(
                null,
                new TableIdent(schemaName, "test"),
                clusterService,
                mock(TransportPutIndexTemplateAction.class),
                executorService,
                false
        );

        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "Table '%s.test' unknown", schemaName));
        builder.build();

    }
}
