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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;

import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.NodeContext;

public class GatewayMetaStateTests extends ESTestCase {

    public void testNoMetadataUpgrade() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false));
        assertThat(upgrade).isSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isTrue();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
        }
    }

    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false));
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("custom meta data too old");
        }
    }

    public void testIndexMetadataUpgrade() {
        Metadata metadata = randomMetadata();
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(true));
        assertThat(upgrade).isNotSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isFalse();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isFalse();
        }
    }

    public void testCustomMetadataNoChange() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false));
        assertThat(upgrade).isSameAs(metadata);
        assertThat(Metadata.isGlobalStateEquals(upgrade, metadata)).isTrue();
        for (IndexMetadata indexMetadata : upgrade) {
            assertThat(metadata.hasIndexMetadata(indexMetadata)).isTrue();
        }
    }

    public static class MockMetadataIndexUpgradeService extends MetadataUpgradeService {
        private final boolean upgrade;

        public MockMetadataIndexUpgradeService(boolean upgrade) {
            super(mock(NodeContext.class), null, mock(UserDefinedFunctionService.class));
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata, IndexTemplateMetadata indexTemplateMetadata, Version minimumIndexCompatibilityVersion, UserDefinedFunctionsMetadata userDefinedFunctionsMetadata) {
            return upgrade ? IndexMetadata.builder(indexMetadata).build() : indexMetadata;
        }
    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        protected CustomMetadata1(String data) {
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

    public static Metadata randomMetadata(TestCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder();
        for (TestCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static List<String> randomIndexPatterns() {
        return Arrays.asList(Objects.requireNonNull(generateRandomStringArray(10, 100, false, false)));
    }
}
