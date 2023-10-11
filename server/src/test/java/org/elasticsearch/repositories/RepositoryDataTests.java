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

package org.elasticsearch.repositories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

/**
 * Tests for the {@link RepositoryData} class.
 */
public class RepositoryDataTests extends ESTestCase {

    @Test
    public void testEqualsAndHashCode() {
        RepositoryData repositoryData1 = generateRandomRepoData();
        RepositoryData repositoryData2 = repositoryData1.copy();
        assertThat(repositoryData1).isEqualTo(repositoryData2);
        assertThat(repositoryData1.hashCode()).isEqualTo(repositoryData2.hashCode());
    }

    @Test
    public void testIndicesToUpdateAfterRemovingSnapshot() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final List<IndexId> indicesBefore = new ArrayList<>(repositoryData.getIndices().values());
        final SnapshotId randomSnapshot = randomFrom(repositoryData.getSnapshotIds());
        final IndexId[] indicesToUpdate = indicesBefore.stream().filter(index -> {
            final List<SnapshotId> snapshotIds = repositoryData.getSnapshots(index);
            return snapshotIds.contains(randomSnapshot) && snapshotIds.size() > 1;
        }).toArray(IndexId[]::new);
        assertThat(repositoryData.indicesToUpdateAfterRemovingSnapshot(
            Collections.singleton(randomSnapshot))).containsExactlyInAnyOrder(indicesToUpdate);
    }

    @Test
    public void testXContent() throws IOException {
        RepositoryData repositoryData = generateRandomRepoData();
        XContentBuilder builder = JsonXContent.builder();
        repositoryData.snapshotsToXContent(builder, Version.CURRENT);
        try (XContentParser parser = createParser(JsonXContent.JSON_XCONTENT, BytesReference.bytes(builder))) {
            long gen = (long) randomIntBetween(0, 500);
            RepositoryData fromXContent = RepositoryData.snapshotsFromXContent(parser, gen, randomBoolean());
            assertThat(repositoryData).isEqualTo(fromXContent);
            assertThat(gen).isEqualTo(fromXContent.getGenId());
        }
    }

    @Test
    public void testAddSnapshots() {
        RepositoryData repositoryData = generateRandomRepoData();
        // test that adding the same snapshot id to the repository data throws an exception
        Map<String, IndexId> indexIdMap = repositoryData.getIndices();
        // test that adding a snapshot and its indices works
        SnapshotId newSnapshot = new SnapshotId(randomAlphaOfLength(7), UUIDs.randomBase64UUID());
        List<IndexId> indices = new ArrayList<>();
        Set<IndexId> newIndices = new HashSet<>();
        int numNew = randomIntBetween(1, 10);
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        for (int i = 0; i < numNew; i++) {
            IndexId indexId = new IndexId(randomAlphaOfLength(7), UUIDs.randomBase64UUID());
            newIndices.add(indexId);
            indices.add(indexId);
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        int numOld = randomIntBetween(1, indexIdMap.size());
        List<String> indexNames = new ArrayList<>(indexIdMap.keySet());
        for (int i = 0; i < numOld; i++) {
            final IndexId indexId = indexIdMap.get(indexNames.get(i));
            indices.add(indexId);
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        final ShardGenerations shardGenerations = builder.build();
        final Map<IndexId, String> indexLookup =
            shardGenerations.indices().stream().collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
        RepositoryData newRepoData = repositoryData.addSnapshot(newSnapshot,
            randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
            shardGenerations, indexLookup, indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random()))));
        // verify that the new repository data has the new snapshot and its indices
        assertThat(newRepoData.getSnapshotIds()).contains(newSnapshot);
        for (IndexId indexId : indices) {
            List<SnapshotId> snapshotIds = newRepoData.getSnapshots(indexId);
            assertThat(snapshotIds).contains(newSnapshot);
            if (newIndices.contains(indexId)) {
                assertThat(snapshotIds).hasSize(1); // if it was a new index, only the new snapshot should be in its set
            }
        }
        assertThat(repositoryData.getGenId()).isEqualTo(newRepoData.getGenId());
    }

    @Test
    public void testInitIndices() {
        final int numSnapshots = randomIntBetween(1, 30);
        final Map<String, SnapshotId> snapshotIds = new HashMap<>(numSnapshots);
        final Map<String, SnapshotState> snapshotStates = new HashMap<>(numSnapshots);
        final Map<String, Version> snapshotVersions = new HashMap<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotStates.put(snapshotId.getUUID(), randomFrom(SnapshotState.values()));
            snapshotVersions.put(snapshotId.getUUID(), randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()));
        }
        RepositoryData repositoryData = new RepositoryData(EMPTY_REPO_GEN, snapshotIds, Collections.emptyMap(), Collections.emptyMap(),
                                                           Collections.emptyMap(), ShardGenerations.EMPTY,  IndexMetaDataGenerations.EMPTY);
        // test that initializing indices works
        Map<IndexId, List<SnapshotId>> indices = randomIndices(snapshotIds);
        RepositoryData newRepoData = new RepositoryData(repositoryData.getGenId(), snapshotIds, snapshotStates, snapshotVersions, indices,
                                                        ShardGenerations.EMPTY, IndexMetaDataGenerations.EMPTY);
        List<SnapshotId> expected = new ArrayList<>(repositoryData.getSnapshotIds());
        Collections.sort(expected);
        List<SnapshotId> actual = new ArrayList<>(newRepoData.getSnapshotIds());
        Collections.sort(actual);
        assertThat(expected).isEqualTo(actual);
        for (IndexId indexId : indices.keySet()) {
            assertThat(indices.get(indexId)).isEqualTo(newRepoData.getSnapshots(indexId));
        }
    }

    @Test
    public void testRemoveSnapshot() {
        RepositoryData repositoryData = generateRandomRepoData();
        List<SnapshotId> snapshotIds = new ArrayList<>(repositoryData.getSnapshotIds());
        assertThat(snapshotIds).isNotEmpty();
        SnapshotId removedSnapshotId = snapshotIds.remove(randomIntBetween(0, snapshotIds.size() - 1));
        RepositoryData newRepositoryData =
            repositoryData.removeSnapshots(Collections.singleton(removedSnapshotId), ShardGenerations.EMPTY);
        // make sure the repository data's indices no longer contain the removed snapshot
        for (final IndexId indexId : newRepositoryData.getIndices().values()) {
            assertThat(newRepositoryData.getSnapshots(indexId)).doesNotContain(removedSnapshotId);
        }
    }

    @Test
    public void testResolveIndexId() {
        RepositoryData repositoryData = generateRandomRepoData();
        Map<String, IndexId> indices = repositoryData.getIndices();
        Set<String> indexNames = indices.keySet();
        assertThat(indexNames).isNotEmpty();
        String indexName = indexNames.iterator().next();
        IndexId indexId = indices.get(indexName);
        assertThat(indexId).isEqualTo(repositoryData.resolveIndexId(indexName));
    }

    @Test
    public void testGetSnapshotState() {
        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
        final SnapshotState state = randomFrom(SnapshotState.values());
        final RepositoryData repositoryData =
            RepositoryData.EMPTY.addSnapshot(snapshotId, state, randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
                ShardGenerations.EMPTY, Collections.emptyMap(), Collections.emptyMap());
        assertThat(state).isEqualTo(repositoryData.getSnapshotState(snapshotId));
        assertThat(repositoryData.getSnapshotState(new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()))).isNull();
    }

    @Test
    public void testIndexThatReferencesAnUnknownSnapshot() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        final RepositoryData repositoryData = generateRandomRepoData();

        XContentBuilder builder = XContentBuilder.builder(xContent);
        repositoryData.snapshotsToXContent(builder, Version.CURRENT);
        RepositoryData parsedRepositoryData;
        try (XContentParser xParser = createParser(builder)) {
            parsedRepositoryData = RepositoryData.snapshotsFromXContent(xParser, repositoryData.getGenId(), randomBoolean());
        }
        assertThat(repositoryData).isEqualTo(parsedRepositoryData);

        Map<String, SnapshotId> snapshotIds = new HashMap<>();
        Map<String, SnapshotState> snapshotStates = new HashMap<>();
        Map<String, Version> snapshotVersions = new HashMap<>();
        for (SnapshotId snapshotId : parsedRepositoryData.getSnapshotIds()) {
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotStates.put(snapshotId.getUUID(), parsedRepositoryData.getSnapshotState(snapshotId));
            snapshotVersions.put(snapshotId.getUUID(), parsedRepositoryData.getVersion(snapshotId));
        }

        final IndexId corruptedIndexId = randomFrom(parsedRepositoryData.getIndices().values());

        Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        final ShardGenerations.Builder shardGenBuilder = ShardGenerations.builder();
        for (Map.Entry<String, IndexId> snapshottedIndex : parsedRepositoryData.getIndices().entrySet()) {
            IndexId indexId = snapshottedIndex.getValue();
            List<SnapshotId> snapshotsIds = new ArrayList<>(parsedRepositoryData.getSnapshots(indexId));
            if (corruptedIndexId.equals(indexId)) {
                snapshotsIds.add(new SnapshotId("_uuid", "_does_not_exist"));
            }
            indexSnapshots.put(indexId, snapshotsIds);
            final int shardCount = randomIntBetween(1, 10);
            for (int i = 0; i < shardCount; ++i) {
                shardGenBuilder.put(indexId, i, UUIDs.randomBase64UUID(random()));
            }
        }
        assertThat(corruptedIndexId).isNotNull();

        RepositoryData corruptedRepositoryData = new RepositoryData(parsedRepositoryData.getGenId(), snapshotIds, snapshotStates,
            snapshotVersions, indexSnapshots, shardGenBuilder.build(), IndexMetaDataGenerations.EMPTY);

        final XContentBuilder corruptedBuilder = XContentBuilder.builder(xContent);
        corruptedRepositoryData.snapshotsToXContent(corruptedBuilder, Version.CURRENT);

        try (XContentParser xParser = createParser(corruptedBuilder)) {
            assertThatThrownBy(() -> RepositoryData.snapshotsFromXContent(xParser, corruptedRepositoryData.getGenId(), randomBoolean()))
                .isExactlyInstanceOf(ElasticsearchParseException.class)
                .hasMessage(
                    "Detected a corrupted repository, index "
                    + corruptedIndexId
                    + " references an unknown snapshot uuid [_does_not_exist]");
        }
    }

    @Test
    public void testIndexThatReferenceANullSnapshot() throws IOException {
        final XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.JSON).xContent());
        builder.startObject();
        {
            builder.startArray("snapshots");
            builder.value(new SnapshotId("_name", "_uuid"));
            builder.endArray();

            builder.startObject("indices");
            {
                builder.startObject("docs");
                {
                    builder.field("id", "_id");
                    builder.startArray("snapshots");
                    {
                        builder.startObject();
                        if (randomBoolean()) {
                            builder.field("name", "_name");
                        }
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        try (XContentParser xParser = createParser(builder)) {
            assertThatThrownBy(() -> RepositoryData.snapshotsFromXContent(xParser, randomNonNegativeLong(), randomBoolean()))
                .isExactlyInstanceOf(ElasticsearchParseException.class)
                .hasMessage(
                    "Detected a corrupted repository, index [docs/_id] references an unknown snapshot uuid [null]");
        }
    }

    // Test removing snapshot from random data where no two snapshots share any index metadata blobs
    @Test
    public void testIndexMetaDataToRemoveAfterRemovingSnapshotNoSharing() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final SnapshotId snapshotId = randomFrom(repositoryData.getSnapshotIds());
        final IndexMetaDataGenerations indexMetaDataGenerations = repositoryData.indexMetaDataGenerations();
        final Collection<IndexId> indicesToUpdate = repositoryData.indicesToUpdateAfterRemovingSnapshot(Collections.singleton(snapshotId));
        final Map<IndexId, Collection<String>> identifiersToRemove = indexMetaDataGenerations.lookup.get(snapshotId).entrySet().stream()
            .filter(e -> indicesToUpdate.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey,
                                      e -> Collections.singleton(indexMetaDataGenerations.getIndexMetaBlobId(e.getValue()))));
        assertThat(repositoryData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(snapshotId))).isEqualTo(identifiersToRemove);
    }

    // Test removing snapshot from random data that has some or all index metadata shared
    @Test
    public void testIndexMetaDataToRemoveAfterRemovingSnapshotWithSharing() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        final SnapshotId otherSnapshotId = randomFrom(repositoryData.getSnapshotIds());
        final Collection<IndexId> indicesInOther = repositoryData.getIndices().values()
            .stream()
            .filter(index -> repositoryData.getSnapshots(index).contains(otherSnapshotId))
            .collect(Collectors.toSet());
        for (IndexId indexId : indicesInOther) {
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        final Map<IndexId, String> newIndices = new HashMap<>();
        final Map<String, String> newIdentifiers = new HashMap<>();
        final Map<IndexId, Collection<String>> removeFromOther = new HashMap<>();
        for (IndexId indexId : randomSubsetOf(repositoryData.getIndices().values())) {
            if (indicesInOther.contains(indexId)) {
                removeFromOther.put(indexId, Collections.singleton(
                    repositoryData.indexMetaDataGenerations().indexMetaBlobId(otherSnapshotId, indexId)));
            }
            final String identifier = randomAlphaOfLength(20);
            newIndices.put(indexId, identifier);
            newIdentifiers.put(identifier, UUIDs.randomBase64UUID(random()));
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        final ShardGenerations shardGenerations = builder.build();
        final Map<IndexId, String> indexLookup = new HashMap<>(repositoryData.indexMetaDataGenerations().lookup.get(otherSnapshotId));
        indexLookup.putAll(newIndices);
        final SnapshotId newSnapshot = new SnapshotId(randomAlphaOfLength(7), UUIDs.randomBase64UUID(random()));

        RepositoryData newRepoData =
            repositoryData.addSnapshot(newSnapshot, SnapshotState.SUCCESS, Version.CURRENT, shardGenerations, indexLookup, newIdentifiers);
        assertEquals(newRepoData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(newSnapshot)),
                     newIndices.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                                                             e -> Collections.singleton(newIdentifiers.get(e.getValue())))));
        assertThat(newRepoData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(otherSnapshotId))).isEqualTo(removeFromOther);
    }

    public static RepositoryData generateRandomRepoData() {
        final int numIndices = randomIntBetween(1, 30);
        final List<IndexId> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()));
        }
        final int numSnapshots = randomIntBetween(1, 30);
        RepositoryData repositoryData = RepositoryData.EMPTY;
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            final List<IndexId> someIndices = indices.subList(0, randomIntBetween(1, numIndices));
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (IndexId someIndex : someIndices) {
                final int shardCount = randomIntBetween(1, 10);
                for (int j = 0; j < shardCount; ++j) {
                    final String uuid = randomBoolean() ? null : UUIDs.randomBase64UUID(random());
                    builder.put(someIndex, j, uuid);
                }
            }
            final Map<IndexId, String> indexLookup =
                someIndices.stream().collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
            repositoryData = repositoryData.addSnapshot(
                snapshotId, randomFrom(SnapshotState.values()),
                randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
                builder.build(),
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random()))));
        }
        return repositoryData;
    }

    private static Map<IndexId, List<SnapshotId>> randomIndices(final Map<String, SnapshotId> snapshotIdsMap) {
        final List<SnapshotId> snapshotIds = new ArrayList<>(snapshotIdsMap.values());
        final int totalSnapshots = snapshotIds.size();
        final int numIndices = randomIntBetween(1, 30);
        final Map<IndexId, List<SnapshotId>> indices = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            final Set<SnapshotId> indexSnapshots = new LinkedHashSet<>();
            final int numIndicesForSnapshot = randomIntBetween(1, numIndices);
            for (int j = 0; j < numIndicesForSnapshot; j++) {
                indexSnapshots.add(snapshotIds.get(randomIntBetween(0, totalSnapshots - 1)));
            }
            indices.put(indexId, Collections.unmodifiableList(new ArrayList<>(indexSnapshots)));
        }
        return indices;
    }
}
