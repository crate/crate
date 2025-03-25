/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.IndexName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;

public class GatewayMetaStateTests extends ESTestCase {

    @Test
    public void test_no_metadata_upgrade_build_relation_metadata() {
        Metadata metadata = randomMetadata(false, new CustomMetadata("data"));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false));
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
            RelationName relationName = IndexName.decode(indexMetadata.getIndex().getName()).toRelationName();
            RelationMetadata.Table relationMetadata = upgrade.getRelation(relationName);
            assertThat(relationMetadata.settings()).isEqualTo(indexMetadata.getSettings());
        }
    }

    @Test
    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(false, new CustomMetadata("data"));
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false));
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("custom meta data too old");
        }
    }

    @Test
    public void test_index_metadata_upgrade_and_build_relation_metadata() {
        Metadata metadata = randomMetadata(false);
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(true));
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isFalse();
            RelationName relationName = IndexName.decode(indexMetadata.getIndex().getName()).toRelationName();
            RelationMetadata.Table relationMetadata = upgrade.getRelation(relationName);
            assertThat(relationMetadata.settings()).isEqualTo(indexMetadata.getSettings());
        }
    }

    @Test
    public void test_index_metadata_upgrade_and_update_relation_metadata() {
        Metadata metadata = randomMetadata(true);
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(true));
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isFalse();
            RelationName relationName = IndexName.decode(indexMetadata.getIndex().getName()).toRelationName();
            RelationMetadata.Table relationMetadata = upgrade.getRelation(relationName);
            assertThat(relationMetadata.settings()).isEqualTo(indexMetadata.getSettings());
        }
    }

    public static class MockMetadataIndexUpgradeService extends MetadataUpgradeService {
        private final boolean upgrade;

        public MockMetadataIndexUpgradeService(boolean upgrade) {
            super(mock(NodeContext.class), null, mock(UserDefinedFunctionService.class));
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata,
                                                  IndexTemplateMetadata indexTemplateMetadata,
                                                  Version minimumIndexCompatibilityVersion,
                                                  UserDefinedFunctionsMetadata userDefinedFunctionsMetadata) {
            if (upgrade) {
                Settings newSettings = Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),  new TimeValue(999))
                    .build();
                return IndexMetadata.builder(indexMetadata).settings(newSettings).build();
            }
            return indexMetadata;
        }
    }

    private static class CustomMetadata extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        protected CustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static Metadata randomMetadata(boolean withRelationMetadata, TestCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder();
        for (TestCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            String indexName = randomAlphaOfLength(10);
            builder.put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
            if (withRelationMetadata) {
                builder.setTable(
                    IndexName.decode(indexName).toRelationName(),
                    List.of(),
                    builder.get(indexName).getSettings(),
                    null,
                    ColumnPolicy.STRICT,
                    null,
                    Map.of(),
                    List.of(),
                    List.of(),
                    IndexMetadata.State.OPEN,
                    List.of());
            }
        }
        return builder.build();
    }
}
