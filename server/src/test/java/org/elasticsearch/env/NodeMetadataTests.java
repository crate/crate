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
package org.elasticsearch.env;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.Version;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

import io.crate.common.collections.Tuple;

public class NodeMetadataTests extends ESTestCase {
    private Version randomVersion() {
        // VersionUtils.randomVersion() only returns known versions, which are necessarily no later than Version.CURRENT; however we want
        // also to consider our behaviour with all versions, so occasionally pick up a truly random version.
        return rarely() ? Version.fromId(randomInt()) : VersionUtils.randomVersion(random());
    }

    @Test
    public void testEqualsHashcodeSerialization() {
        final Path tempDir = createTempDir();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new NodeMetadata(randomAlphaOfLength(10), randomVersion()),
                                                       nodeMetadata -> {
                                                           final long generation = NodeMetadata.FORMAT.writeAndCleanup(nodeMetadata, tempDir);
                                                           final Tuple<NodeMetadata, Long> nodeMetadataLongTuple
                                                               = NodeMetadata.FORMAT.loadLatestStateWithGeneration(logger, xContentRegistry(), tempDir);
                                                           assertThat(nodeMetadataLongTuple.v2(), equalTo(generation));
                                                           return nodeMetadataLongTuple.v1();
                                                       }, nodeMetadata -> {
                if (randomBoolean()) {
                    return new NodeMetadata(randomAlphaOfLength(21 - nodeMetadata.nodeId().length()), nodeMetadata.nodeVersion());
                } else {
                    return new NodeMetadata(nodeMetadata.nodeId(), randomValueOtherThan(nodeMetadata.nodeVersion(), this::randomVersion));
                }
            });
    }

    @Test
    public void testReadsFormatWithoutVersion() throws IOException, URISyntaxException {
        // the behaviour tested here is only appropriate if the current version is compatible with versions 7 and earlier
        assertTrue(Version.CURRENT.minimumIndexCompatibilityVersion().onOrBefore(Version.V_4_0_0));
        // when the current version is incompatible with version 4, the behaviour should change to reject files like the given resource
        // which do not have the version field

        final Path tempDir = createTempDir();
        final Path stateDir = Files.createDirectory(tempDir.resolve(MetadataStateFormat.STATE_DIR_NAME));
        final InputStream resource = this.getClass().getResourceAsStream("testReadsFormatWithoutVersion.binary");
        assertThat(resource, notNullValue());
        Files.copy(resource, stateDir.resolve(NodeMetadata.FORMAT.getStateFileName(between(0, Integer.MAX_VALUE))));
        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempDir);
        assertThat(nodeMetadata.nodeId(), equalTo("y6VUVMSaStO4Tz-B5BxcOw"));
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.V_EMPTY));
    }

    @Test
    public void testUpgradesLegitimateVersions() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId,
                                                           randomValueOtherThanMany(v -> v.after(Version.CURRENT) || v.before(Version.CURRENT.minimumIndexCompatibilityVersion()),
                                                                                    this::randomVersion)).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    @Test
    public void testUpgradesMissingVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId, Version.V_EMPTY).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    @Test
    public void testDoesNotUpgradeFutureVersion() {
        assertThatThrownBy(() -> new NodeMetadata(randomAlphaOfLength(10), tooNewVersion()) .upgradeToCurrentVersion())
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("cannot downgrade a node from version [")
            .hasMessageEndingWith("] to version [" + Version.CURRENT + "]");
    }

    @Test
    public void testDoesNotUpgradeAncientVersion() {
        assertThatThrownBy(() -> new NodeMetadata(randomAlphaOfLength(10), tooOldVersion()).upgradeToCurrentVersion())
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("cannot upgrade a node from version [")
            .hasMessageEndingWith("] directly to version [" + Version.CURRENT + "]");
    }

    public static Version tooNewVersion() {
        return Version.fromId(between(Version.CURRENT.internalId + 10000, 99999999));
    }

    public static Version tooOldVersion() {
        return Version.fromId(between(1, Version.CURRENT.minimumIndexCompatibilityVersion().internalId - 1));
    }

    @Test
    public void test_downgrade_hotfix_version() {
        var nodeMetadata = new NodeMetadata("test", Version.V_4_5_1);
        nodeMetadata = nodeMetadata.upgradeToVersion(Version.V_4_5_0);
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.V_4_5_0));
    }
}
