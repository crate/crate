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
package org.elasticsearch.cluster.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;

public class CoordinationMetadataTests extends ESTestCase {

    public void testVotingConfiguration() {
        VotingConfiguration config0 = new VotingConfiguration(Set.of());
        assertThat(config0).isEqualTo(VotingConfiguration.EMPTY_CONFIG);
        assertThat(config0.getNodeIds()).isEqualTo(Set.of());
        assertThat(config0.isEmpty()).isEqualTo(true);
        assertThat(config0.hasQuorum(Set.of())).isEqualTo(false);
        assertThat(config0.hasQuorum(Set.of("id1"))).isEqualTo(false);

        VotingConfiguration config1 = new VotingConfiguration(Set.of("id1"));
        assertThat(config1.getNodeIds()).isEqualTo(Set.of("id1"));
        assertThat(config1.isEmpty()).isEqualTo(false);
        assertThat(config1.hasQuorum(Set.of("id1"))).isEqualTo(true);
        assertThat(config1.hasQuorum(Set.of("id1", "id2"))).isEqualTo(true);
        assertThat(config1.hasQuorum(Set.of("id2"))).isEqualTo(false);
        assertThat(config1.hasQuorum(Set.of())).isEqualTo(false);

        VotingConfiguration config2 = new VotingConfiguration(Set.of("id1", "id2"));
        assertThat(config2.getNodeIds()).isEqualTo(Set.of("id1", "id2"));
        assertThat(config2.isEmpty()).isEqualTo(false);
        assertThat(config2.hasQuorum(Set.of("id1", "id2"))).isEqualTo(true);
        assertThat(config2.hasQuorum(Set.of("id1", "id2", "id3"))).isEqualTo(true);
        assertThat(config2.hasQuorum(Set.of("id1"))).isEqualTo(false);
        assertThat(config2.hasQuorum(Set.of("id2"))).isEqualTo(false);
        assertThat(config2.hasQuorum(Set.of("id3"))).isEqualTo(false);
        assertThat(config2.hasQuorum(Set.of("id1", "id3"))).isEqualTo(false);
        assertThat(config2.hasQuorum(Set.of())).isEqualTo(false);

        VotingConfiguration config3 = new VotingConfiguration(Set.of("id1", "id2", "id3"));
        assertThat(config3.getNodeIds()).isEqualTo(Set.of("id1", "id2", "id3"));
        assertThat(config3.isEmpty()).isEqualTo(false);
        assertThat(config3.hasQuorum(Set.of("id1", "id2"))).isEqualTo(true);
        assertThat(config3.hasQuorum(Set.of("id2", "id3"))).isEqualTo(true);
        assertThat(config3.hasQuorum(Set.of("id1", "id3"))).isEqualTo(true);
        assertThat(config3.hasQuorum(Set.of("id1", "id2", "id3"))).isEqualTo(true);
        assertThat(config3.hasQuorum(Set.of("id1", "id2", "id4"))).isEqualTo(true);
        assertThat(config3.hasQuorum(Set.of("id1"))).isEqualTo(false);
        assertThat(config3.hasQuorum(Set.of("id2"))).isEqualTo(false);
        assertThat(config3.hasQuorum(Set.of("id3"))).isEqualTo(false);
        assertThat(config3.hasQuorum(Set.of("id1", "id4"))).isEqualTo(false);
        assertThat(config3.hasQuorum(Set.of("id1", "id4", "id5"))).isEqualTo(false);
        assertThat(config3.hasQuorum(Set.of())).isEqualTo(false);
    }

    public void testVotingConfigurationSerializationEqualsHashCode() {
        VotingConfiguration initialConfig = randomVotingConfig();
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialConfig,
                (CopyFunction<VotingConfiguration>) orig -> ESTestCase.copyWriteable(orig,
                        new NamedWriteableRegistry(Collections.emptyList()), VotingConfiguration::new),
                cfg -> randomlyChangeVotingConfiguration(cfg));
    }

    private static VotingConfiguration randomVotingConfig() {
        return new VotingConfiguration(Set.of(generateRandomStringArray(randomInt(10), 20, false)));
    }

    public void testVotingTombstoneSerializationEqualsHashCode() {
        VotingConfigExclusion tombstone = new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10));
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(tombstone,
                (CopyFunction<VotingConfigExclusion>) orig -> ESTestCase.copyWriteable(orig,
                        new NamedWriteableRegistry(Collections.emptyList()), VotingConfigExclusion::new),
                orig -> randomlyChangeVotingTombstone(orig));
    }

    public void testVotingTombstoneXContent() throws IOException {
        VotingConfigExclusion originalTombstone = new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10));

        final XContentBuilder builder = JsonXContent.builder();
        originalTombstone.toXContent(builder, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = createParser(JsonXContent.JSON_XCONTENT, BytesReference.bytes(builder))) {
            final VotingConfigExclusion fromXContentTombstone = VotingConfigExclusion.fromXContent(parser);
            assertThat(originalTombstone).isEqualTo(fromXContentTombstone);
        }
    }

    private VotingConfigExclusion randomlyChangeVotingTombstone(VotingConfigExclusion tombstone) {
        if (randomBoolean()) {
            return new VotingConfigExclusion(randomAlphaOfLength(10), tombstone.getNodeName());
        } else {
            return new VotingConfigExclusion(tombstone.getNodeId(), randomAlphaOfLength(10));
        }
    }

    private VotingConfiguration randomlyChangeVotingConfiguration(VotingConfiguration cfg) {
        Set<String> newNodeIds = new HashSet<>(cfg.getNodeIds());
        if (cfg.isEmpty() == false && randomBoolean()) {
            // remove random element
            newNodeIds.remove(randomFrom(cfg.getNodeIds()));
        } else if (cfg.isEmpty() == false && randomBoolean()) {
            // change random element
            newNodeIds.remove(randomFrom(cfg.getNodeIds()));
            newNodeIds.add(randomAlphaOfLength(20));
        } else {
            // add random element
            newNodeIds.add(randomAlphaOfLength(20));
        }
        return new VotingConfiguration(newNodeIds);
    }

    private Set<VotingConfigExclusion> randomVotingTombstones() {
        final int size = randomIntBetween(1, 10);
        final Set<VotingConfigExclusion> nodes = new HashSet<>(size);
        while (nodes.size() < size) {
            assertThat(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10)))).isTrue();
        }
        return nodes;
    }

    public void testCoordinationMetadataSerializationEqualsHashCode() {
        CoordinationMetadata initialMetadata = new CoordinationMetadata(randomNonNegativeLong(), randomVotingConfig(), randomVotingConfig(),
                randomVotingTombstones());
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialMetadata,
                (CopyFunction<CoordinationMetadata>) orig -> ESTestCase.copyWriteable(orig,
                        new NamedWriteableRegistry(Collections.emptyList()), CoordinationMetadata::new),
            meta -> {
                CoordinationMetadata.Builder builder = CoordinationMetadata.builder(meta);
                switch (randomInt(3)) {
                    case 0:
                        builder.term(randomValueOtherThan(meta.term(), ESTestCase::randomNonNegativeLong));
                        break;
                    case 1:
                        builder.lastCommittedConfiguration(randomlyChangeVotingConfiguration(meta.getLastCommittedConfiguration()));
                        break;
                    case 2:
                        builder.lastAcceptedConfiguration(randomlyChangeVotingConfiguration(meta.getLastAcceptedConfiguration()));
                        break;
                    case 3:
                        if (meta.getVotingConfigExclusions().isEmpty() == false && randomBoolean()) {
                            builder.clearVotingConfigExclusions();
                        } else {
                            randomVotingTombstones().forEach(dn -> builder.addVotingConfigExclusion(dn));
                        }
                        break;
                }
                return builder.build();
            });
    }

    public void testXContent() throws IOException {
        CoordinationMetadata originalMeta = new CoordinationMetadata(randomNonNegativeLong(), randomVotingConfig(), randomVotingConfig(),
                randomVotingTombstones());

        final XContentBuilder builder = JsonXContent.builder();
        builder.startObject();
        originalMeta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.JSON_XCONTENT, BytesReference.bytes(builder))) {
            final CoordinationMetadata fromXContentMeta = CoordinationMetadata.fromXContent(parser);
            assertThat(originalMeta).isEqualTo(fromXContentMeta);
        }
    }
}
