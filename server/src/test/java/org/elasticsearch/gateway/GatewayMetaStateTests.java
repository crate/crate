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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class GatewayMetaStateTests extends ESTestCase {

    public void testAddCustomMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetadata1.TYPE, new CustomMetadata1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.custom(CustomMetadata1.TYPE));
        assertThat(((TestCustomMetadata) upgrade.custom(CustomMetadata1.TYPE)).getData(), equalTo("modified_data1"));
    }

    public void testRemoveCustomMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.singletonList(customs -> {
                customs.remove(CustomMetadata1.TYPE);
                return customs;
            }),
            Collections.emptyList()
        );
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNull(upgrade.custom(CustomMetadata1.TYPE));
    }

    public void testUpdateCustomMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetadata1.TYPE, new CustomMetadata1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );

        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.custom(CustomMetadata1.TYPE));
        assertThat(((TestCustomMetadata) upgrade.custom(CustomMetadata1.TYPE)).getData(), equalTo("modified_data1"));
    }


    public void testUpdateTemplateMetadataOnUpgrade() throws Exception {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.emptyList(),
            Collections.singletonList(
                templates -> {
                    templates.put("added_test_template", IndexTemplateMetadata.builder("added_test_template")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false))).build());
                    return templates;
                }
            ));

        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertTrue(upgrade.templates().containsKey("added_test_template"));
    }

    public void testNoMetadataUpgrade() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList(), Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade == metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataValidation() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(
            customs -> {
                throw new IllegalStateException("custom meta data too old");
            }
        ), Collections.emptyList());
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testMultipleCustomMetadataUpgrade() throws Exception {
        final Metadata metadata;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metadata = randomMetadata(new CustomMetadata1("data1"), new CustomMetadata2("data2"));
                break;
            case 1:
                metadata = randomMetadata(randomBoolean() ? new CustomMetadata1("data1") : new CustomMetadata2("data2"));
                break;
            case 2:
                metadata = randomMetadata();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Arrays.asList(
                customs -> {
                    customs.put(CustomMetadata1.TYPE, new CustomMetadata1("modified_data1"));
                    return customs;
                },
                customs -> {
                    customs.put(CustomMetadata2.TYPE, new CustomMetadata1("modified_data2"));
                    return customs;
                }
            ), Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.custom(CustomMetadata1.TYPE));
        assertThat(((TestCustomMetadata) upgrade.custom(CustomMetadata1.TYPE)).getData(), equalTo("modified_data1"));
        assertNotNull(upgrade.custom(CustomMetadata2.TYPE));
        assertThat(((TestCustomMetadata) upgrade.custom(CustomMetadata2.TYPE)).getData(), equalTo("modified_data2"));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testIndexMetadataUpgrade() throws Exception {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList(), Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(true), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertFalse(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataNoChange() throws Exception {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(HashMap::new),
            Collections.singletonList(HashMap::new));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade == metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testIndexTemplateValidation() throws Exception {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.emptyList(),
            Collections.singletonList(
                customs -> {
                    throw new IllegalStateException("template is incompatible");
                }));
        String message = expectThrows(IllegalStateException.class,
            () -> GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader)).getMessage();
        assertThat(message, equalTo("template is incompatible"));
    }


    public void testMultipleIndexTemplateUpgrade() throws Exception {
        final Metadata metadata;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metadata = randomMetadataWithIndexTemplates("template1", "template2");
                break;
            case 1:
                metadata = randomMetadataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
                break;
            case 2:
                metadata = randomMetadata();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(
            Collections.emptyList(),
            Arrays.asList(
                indexTemplateMetadatas -> {
                    indexTemplateMetadatas.put("template1", IndexTemplateMetadata.builder("template1")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                        .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                        .build());
                    return indexTemplateMetadatas;

                },
                indexTemplateMetadatas -> {
                    indexTemplateMetadatas.put("template2", IndexTemplateMetadata.builder("template2")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                        .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build()).build());
                    return indexTemplateMetadatas;

                }
            ));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertTrue(upgrade != metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.templates().get("template1"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings()), equalTo(20));
        assertNotNull(upgrade.templates().get("template2"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings()), equalTo(10));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    private static class MockMetadataIndexUpgradeService extends MetadataIndexUpgradeService {
        private final boolean upgrade;

        MockMetadataIndexUpgradeService(boolean upgrade) {
            super(Settings.EMPTY, null, null, null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
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

    private static class CustomMetadata2 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_2";

        protected CustomMetadata2(String data) {
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

    private static Metadata randomMetadata(TestCustomMetadata... customMetadatas) {
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

    private static Metadata randomMetadataWithIndexTemplates(String... templates) {
        Metadata.Builder builder = Metadata.builder();
        for (String template : templates) {
            IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.builder(template)
                .settings(settings(Version.CURRENT)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5)))
                .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                .build();
            builder.put(templateMetadata);
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
}
