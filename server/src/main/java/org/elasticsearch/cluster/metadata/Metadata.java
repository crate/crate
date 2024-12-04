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

package org.elasticsearch.cluster.metadata;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.Diffs;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.expression.symbol.RefReplacer;
import io.crate.fdw.ForeignTablesMetadata;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.sql.tree.ColumnPolicy;

public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata> {

    private static final Logger LOGGER = LogManager.getLogger(Metadata.class);
    public static long COLUMN_OID_UNASSIGNED = 0L;

    public static final String ALL = "_all";
    public static final String UNKNOWN_CLUSTER_UUID = "_na_";

    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT
    }

    /**
     * Indicates that this custom metadata will be returned as part of an API call but will not be persisted
     */
    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and will be persisted between
     * node restarts, but will not be a part of a snapshot global state
     */
    public static EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and stored as a part of
     * a snapshot global state, but will not be persisted between node restarts
     */
    public static EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    /**
     * Indicates that this custom metadata will be returned as part of an API call, stored as a part of
     * a snapshot global state, and will be persisted between node restarts
     */
    public static EnumSet<XContentContext> ALL_CONTEXTS = EnumSet.allOf(XContentContext.class);

    public interface Custom extends NamedDiffable<Custom> {

        EnumSet<XContentContext> context();
    }

    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING =
        Setting.boolSetting("cluster.blocks.read_only", false, Property.Dynamic, Property.NodeScope);

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)", false, false,
        false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING =
        Setting.boolSetting("cluster.blocks.read_only_allow_delete", false, Property.Dynamic, Property.NodeScope);

    public static final ClusterBlock CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(13, "cluster read-only / allow delete (api)",
        false, false, true, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final Metadata EMPTY_METADATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;
    private final long columnOID;

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final ImmutableOpenMap<String, IndexMetadata> indices;
    private final ImmutableOpenMap<String, IndexMetadata> indicesByUUID;
    private final ImmutableOpenMap<String, Custom> customs;
    private final ImmutableOpenMap<String, SchemaMetadata> schemas;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;
    private final int numberOfShards;

    private final String[] allIndices;
    private final String[] allOpenIndices;
    private final String[] allClosedIndices;

    private final SortedMap<String, AliasOrIndex> aliasAndIndexLookup;

    Metadata(String clusterUUID,
             boolean clusterUUIDCommitted,
             long version,
             long columnOID,
             CoordinationMetadata coordinationMetadata,
             Settings transientSettings,
             Settings persistentSettings,
             ImmutableOpenMap<String, IndexMetadata> indices,
             ImmutableOpenMap<String, Custom> customs,
             ImmutableOpenMap<String, SchemaMetadata> schemas,
             String[] allIndices,
             String[] allOpenIndices,
             String[] allClosedIndices,
             SortedMap<String, AliasOrIndex> aliasAndIndexLookup) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.columnOID = columnOID;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.builder().put(persistentSettings).put(transientSettings).build();
        this.indices = indices;
        this.customs = customs;
        this.schemas = schemas;
        int totalNumberOfShards = 0;
        int totalOpenIndexShards = 0;
        int numberOfShards = 0;
        for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
            totalNumberOfShards += cursor.value.getTotalNumberOfShards();
            numberOfShards += cursor.value.getNumberOfShards();
            if (IndexMetadata.State.OPEN.equals(cursor.value.getState())) {
                totalOpenIndexShards += cursor.value.getTotalNumberOfShards();
            }
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenIndexShards = totalOpenIndexShards;
        this.numberOfShards = numberOfShards;

        this.allIndices = allIndices;
        this.allOpenIndices = allOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.aliasAndIndexLookup = aliasAndIndexLookup;

        indicesByUUID = buildIndicesByUUIDMap();
    }

    private ImmutableOpenMap<String, IndexMetadata> buildIndicesByUUIDMap() {
        ImmutableOpenMap.Builder<String, IndexMetadata> builder = ImmutableOpenMap.builder();
        for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
            IndexMetadata indexMetadata = cursor.value;
            builder.put(indexMetadata.getIndexUUID(), indexMetadata);
        }
        return builder.build();
    }

    public long version() {
        return this.version;
    }

    public long columnOID() {
        return this.columnOID;
    }

    public String clusterUUID() {
        return this.clusterUUID;
    }

    /**
     * Whether the current node with the given cluster state is locked into the cluster with the UUID returned by {@link #clusterUUID()},
     * meaning that it will not accept any cluster state with a different clusterUUID.
     */
    public boolean clusterUUIDCommitted() {
        return this.clusterUUIDCommitted;
    }

    /**
     * Returns the merged transient and persistent settings.
     */
    public Settings settings() {
        return this.settings;
    }

    public Settings transientSettings() {
        return this.transientSettings;
    }

    public Settings persistentSettings() {
        return this.persistentSettings;
    }

    public CoordinationMetadata coordinationMetadata() {
        return this.coordinationMetadata;
    }

    public boolean hasAlias(String alias) {
        AliasOrIndex aliasOrIndex = getAliasAndIndexLookup().get(alias);
        if (aliasOrIndex != null) {
            return aliasOrIndex.isAlias();
        } else {
            return false;
        }
    }

    public SortedMap<String, AliasOrIndex> getAliasAndIndexLookup() {
        return aliasAndIndexLookup;
    }

    /**
     * Returns all the concrete indices.
     */
    public String[] getConcreteAllIndices() {
        return allIndices;
    }

    public String[] getConcreteAllOpenIndices() {
        return allOpenIndices;
    }

    public String[] getConcreteAllClosedIndices() {
        return allClosedIndices;
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public boolean hasIndex(Index index) {
        return indicesByUUID.containsKey(index.getUUID());
    }

    public boolean hasConcreteIndex(String index) {
        return getAliasAndIndexLookup().containsKey(index);
    }

    @Nullable
    public IndexMetadata index(String indexName) {
        return indices.get(indexName);
    }

    @Nullable
    public IndexMetadata indexByUUID(String indexUUID) {
        return indicesByUUID.get(indexUUID);
    }

    @Nullable
    public IndexMetadata index(Index index) {
        return indicesByUUID.get(index.getUUID());
    }

    /** Returns true iff existing index has the same {@link IndexMetadata} instance */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getName()) == indexMetadata;
    }

    /**
     * Returns the {@link IndexMetadata} for this index.
     * @throws IndexNotFoundException if no metadata for this index is found
     */
    public IndexMetadata getIndexSafe(Index index) {
        IndexMetadata metadata = index(index);
        if (metadata != null) {
            return metadata;
        }
        throw new IndexNotFoundException(index);
    }

    /**
     * @return indexName -> indexMetadata
     **/
    public ImmutableOpenMap<String, IndexMetadata> indices() {
        return this.indices;
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
    }

    public ImmutableOpenMap<String, SchemaMetadata> schemas() {
        return schemas;
    }

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return custom(IndexGraveyard.TYPE);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return this.totalOpenIndexShards;
    }

    /**
     * Gets the number of primary shards from all indices, not including
     * replicas.
     * @return The number of primary shards from all indices.
     */
    public int getNumberOfShards() {
        return this.numberOfShards;
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.valuesIt();
    }

    public static boolean isGlobalStateEquals(Metadata metadata1, Metadata metadata2) {
        if (!metadata1.coordinationMetadata.equals(metadata2.coordinationMetadata)) {
            return false;
        }
        if (!metadata1.persistentSettings.equals(metadata2.persistentSettings)) {
            return false;
        }
        if (!metadata1.clusterUUID.equals(metadata2.clusterUUID)) {
            return false;
        }
        if (metadata1.clusterUUIDCommitted != metadata2.clusterUUIDCommitted) {
            return false;
        }
        if (metadata1.columnOID != metadata2.columnOID) {
            return false;
        }

        // Check if any persistent metadata needs to be saved
        int customCount1 = 0;
        for (ObjectObjectCursor<String, Custom> cursor : metadata1.customs) {
            if (cursor.value.context().contains(XContentContext.GATEWAY)) {
                if (!cursor.value.equals(metadata2.custom(cursor.key))) return false;
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (ObjectCursor<Custom> cursor : metadata2.customs.values()) {
            if (cursor.value.context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        if (customCount1 != customCount2) return false;
        return true;
    }

    @Override
    public Diff<Metadata> diff(Metadata previousState) {
        return new MetadataDiff(previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        return new MetadataDiff(in);
    }

    public static Metadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser, false);
    }

    private static class MetadataDiff implements Diff<Metadata> {

        private final long version;
        private final long columnOID;

        private final String clusterUUID;
        private final boolean clusterUUIDCommitted;
        private final CoordinationMetadata coordinationMetadata;
        private final Settings transientSettings;
        private final Settings persistentSettings;
        private final Diff<ImmutableOpenMap<String, IndexMetadata>> indices;
        private final Diff<ImmutableOpenMap<String, Custom>> customs;
        private final Diff<ImmutableOpenMap<String, SchemaMetadata>> schemas;

        MetadataDiff(Metadata before, Metadata after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            columnOID = after.columnOID;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            indices = Diffs.diff(before.indices, after.indices, Diffs.stringKeySerializer());
            customs = Diffs.diff(before.customs, after.customs, Diffs.stringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            schemas = Diffs.diff(before.schemas, after.schemas, Diffs.stringKeySerializer());
        }

        private static final Diffs.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final Diffs.DiffableValueReader<String, SchemaMetadata> SCHEMA_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(SchemaMetadata::of, SchemaMetadata::readDiffFrom);

        MetadataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            if (in.getVersion().onOrAfter(Version.V_5_5_0)) {
                columnOID = in.readLong();
            } else {
                columnOID = COLUMN_OID_UNASSIGNED;
            }
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            indices = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            customs = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            if (in.getVersion().onOrAfter(Version.V_5_10_0)) {
                schemas = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), SCHEMA_DIFF_VALUE_READER);
            } else {
                schemas = (Diff<ImmutableOpenMap<String, SchemaMetadata>>) AbstractDiffable.EMPTY;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            if (out.getVersion().onOrAfter(Version.V_5_5_0)) {
                out.writeLong(columnOID);
            }
            coordinationMetadata.writeTo(out);
            Settings.writeSettingsToStream(out, transientSettings);
            Settings.writeSettingsToStream(out, persistentSettings);
            indices.writeTo(out);
            customs.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_5_10_0)) {
                schemas.writeTo(out);
            }
        }

        @Override
        public Metadata apply(Metadata part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.columnOID(columnOID);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.indices(indices.apply(part.indices));
            builder.customs(customs.apply(part.customs));
            builder.schemas.putAll(schemas.apply(part.schemas));
            return builder.build();
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_5_5_0)) {
            builder.columnOID(in.readLong());
        } else {
            builder.columnOID(COLUMN_OID_UNASSIGNED);
        }
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in), false);
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }

        int numSchemas = in.readVInt();
        for (int i = 0; i < numSchemas; i++) {
            String schemaName = in.readString();
            SchemaMetadata schemaMetadata = SchemaMetadata.of(in);
            builder.addSchema(schemaName, schemaMetadata);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        if (out.getVersion().onOrAfter(Version.V_5_5_0)) {
            out.writeLong(columnOID);
        }
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        writeSettingsToStream(out, transientSettings);
        writeSettingsToStream(out, persistentSettings);
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out);
        }
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final ObjectCursor<Custom> cursor : customs.values()) {
            if (VersionedNamedWriteable.shouldSerialize(out, cursor.value)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final ObjectCursor<Custom> cursor : customs.values()) {
            if (VersionedNamedWriteable.shouldSerialize(out, cursor.value)) {
                out.writeNamedWriteable(cursor.value);
            }
        }

        out.writeVInt(schemas.size());
        for (var cursor : schemas) {
            String schemaName = cursor.key;
            out.writeString(schemaName);

            SchemaMetadata schemaMetadata = cursor.value;
            schemaMetadata.writeTo(out);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Metadata metadata) {
        return new Builder(metadata);
    }

    private static class ColumnOidSupplier implements LongSupplier {
        private long columnOID;

        @VisibleForTesting
        public ColumnOidSupplier(long columnOID) {
            this.columnOID = columnOID;
        }

        @Override
        public long getAsLong() {
            columnOID++;
            return columnOID;
        }
    }

    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;
        private ColumnOidSupplier columnOidSupplier;
        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.EMPTY;
        private Settings persistentSettings = Settings.EMPTY;

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, Custom> customs;
        private final ImmutableOpenMap.Builder<String, SchemaMetadata> schemas;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            schemas = ImmutableOpenMap.builder();
            columnOidSupplier = new ColumnOidSupplier(COLUMN_OID_UNASSIGNED);
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.version = metadata.version;
            this.columnOidSupplier = new ColumnOidSupplier(metadata.columnOID);
            this.indices = ImmutableOpenMap.builder(metadata.indices);
            this.customs = ImmutableOpenMap.builder(metadata.customs);
            this.schemas = ImmutableOpenMap.builder(metadata.schemas);
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            assert indices.containsKey(indexMetadata.getIndexUUID()) == false :
                "An index with the same UUID already exists: [" + indexMetadata.getIndexUUID() + "]";
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            if (indices.get(indexMetadata.getIndex().getName()) == indexMetadata) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetadata = IndexMetadata.builder(indexMetadata).version(indexMetadata.getVersion() + 1).build();
            }
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public IndexMetadata get(String index) {
            return indices.get(index);
        }

        public IndexMetadata getSafe(Index index) {
            IndexMetadata indexMetadata = get(index.getName());
            if (indexMetadata != null) {
                if (indexMetadata.getIndexUUID().equals(index.getUUID())) {
                    return indexMetadata;
                }
                throw new IndexNotFoundException(index,
                    new IllegalStateException("index uuid doesn't match expected: [" + index.getUUID()
                        + "] but got: [" + indexMetadata.getIndexUUID() + "]"));
            }
            throw new IndexNotFoundException(index);
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            indices.clear();
            return this;
        }

        public Builder indices(ImmutableOpenMap<String, IndexMetadata> indices) {
            this.indices.putAll(indices);
            return this;
        }

        public Builder addSchema(String schema, SchemaMetadata schemaMetadata) {
            SchemaMetadata existing = schemas.get(schema);
            if (existing == null) {
                schemas.put(schema, schemaMetadata);
            } else {
                ImmutableOpenMap.Builder<String, RelationMetadata> builder = ImmutableOpenMap.builder(existing.relations());
                builder.putAll(schemaMetadata.relations());
                schemas.put(schema, new SchemaMetadata(builder.build()));
            }
            return this;
        }

        public Builder addTable(RelationName name,
                                Collection<Reference> columns,
                                Settings settings,
                                @Nullable ColumnIdent routingColumn,
                                ColumnPolicy columnPolicy,
                                @Nullable String pkConstraintName,
                                Map<String, String> checkConstraints,
                                List<ColumnIdent> primaryKeys,
                                List<ColumnIdent> partitionedBy,
                                IndexMetadata.State state,
                                List<String> indexUUIDs) {
            return addTable(
                columnOidSupplier,
                name,
                columns,
                settings,
                routingColumn,
                columnPolicy,
                pkConstraintName,
                checkConstraints,
                primaryKeys,
                partitionedBy,
                state,
                indexUUIDs
            );
        }

        @VisibleForTesting
        public Builder addTable(@Nullable LongSupplier columnOidSupplier,
                                RelationName relationName,
                                Collection<Reference> columns,
                                Settings settings,
                                @Nullable ColumnIdent routingColumn,
                                ColumnPolicy columnPolicy,
                                @Nullable String pkConstraintName,
                                Map<String, String> checkConstraints,
                                List<ColumnIdent> primaryKeys,
                                List<ColumnIdent> partitionedBy,
                                IndexMetadata.State state,
                                List<String> indexUUIDs) {
            String schema = relationName.schema();
            SchemaMetadata schemaMetadata = schemas.get(schema);
            AtomicInteger positions = new AtomicInteger(0);

            LongSupplier oidSupplier = columnOidSupplier == null ? this.columnOidSupplier : columnOidSupplier;
            Map<ColumnIdent, Reference> columnMap = columns.stream()
                .map(ref -> ref.withOidAndPosition(oidSupplier, positions::incrementAndGet))
                .collect(Collectors.toMap(ref -> ref.column(), ref -> ref));

            ArrayList<Reference> finalColumns = new ArrayList<>(columns.size());
            for (var column : columnMap.values()) {
                Reference newRef;
                if (column instanceof GeneratedReference genRef) {
                    newRef = new GeneratedReference(
                        genRef.reference(),
                        RefReplacer.replaceRefs(genRef.generatedExpression(), ref -> columnMap.get(ref.column()))
                    );
                } else {
                    newRef = column;
                }
                finalColumns.add(newRef);
            }
            RelationMetadata.Table table = new RelationMetadata.Table(
                relationName,
                finalColumns,
                settings,
                routingColumn,
                columnPolicy,
                pkConstraintName,
                checkConstraints,
                primaryKeys,
                partitionedBy,
                state,
                indexUUIDs
            );
            ImmutableOpenMap<String, RelationMetadata> relations;
            if (schemaMetadata == null) {
                relations = ImmutableOpenMap.<String, RelationMetadata>builder(1)
                    .fPut(relationName.name(), table)
                    .build();
            } else {
                relations = ImmutableOpenMap.builder(schemaMetadata.relations())
                    .fPut(relationName.name(), table)
                    .build();
            }
            schemaMetadata = new SchemaMetadata(relations);
            schemas.put(schema, schemaMetadata);
            return this;
        }

        public Custom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, custom);
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(ImmutableOpenMap<String, Custom> customs) {
            this.customs.putAll(customs);
            return this;
        }

        public Builder indexGraveyard(final IndexGraveyard indexGraveyard) {
            putCustom(IndexGraveyard.TYPE, indexGraveyard);
            return this;
        }

        public IndexGraveyard indexGraveyard() {
            IndexGraveyard graveyard = (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
            return graveyard;
        }

        /**
         * Update the number of replicas for the specified indices.
         *
         * @param numberOfReplicas the number of replicas
         * @param indices          the indices to update the number of replicas for
         * @return the builder
         */
        public Builder updateNumberOfReplicas(final int numberOfReplicas, final String[] indices) {
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata).numberOfReplicas(numberOfReplicas));
            }
            return this;
        }

        public Builder coordinationMetadata(CoordinationMetadata coordinationMetadata) {
            this.coordinationMetadata = coordinationMetadata;
            return this;
        }

        public Settings transientSettings() {
            return this.transientSettings;
        }

        public Builder transientSettings(Settings settings) {
            this.transientSettings = settings;
            return this;
        }

        public Settings persistentSettings() {
            return this.persistentSettings;
        }

        public Builder persistentSettings(Settings settings) {
            this.persistentSettings = settings;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder columnOID(long columnOID) {
            this.columnOidSupplier = new ColumnOidSupplier(columnOID);
            return this;
        }

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder clusterUUIDCommitted(boolean clusterUUIDCommitted) {
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            return this;
        }

        public Builder generateClusterUuidIfNeeded() {
            if (clusterUUID.equals(UNKNOWN_CLUSTER_UUID)) {
                clusterUUID = UUIDs.randomBase64UUID();
            }
            return this;
        }

        public ColumnOidSupplier columnOidSupplier() {
            return columnOidSupplier;
        }

        public Metadata build() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will only be rebuilded when needed. Now during serializing we rebuild these datastructures
            //    while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            final Set<String> allIndices = new HashSet<>(indices.size());
            final List<String> allOpenIndices = new ArrayList<>();
            final List<String> allClosedIndices = new ArrayList<>();
            final Set<String> duplicateAliasesIndices = new HashSet<>();
            for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                final IndexMetadata indexMetadata = cursor.value;
                final String name = indexMetadata.getIndex().getName();
                boolean added = allIndices.add(name);
                assert added : "double index named [" + name + "]";
                if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                    allOpenIndices.add(indexMetadata.getIndex().getName());
                } else if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    allClosedIndices.add(indexMetadata.getIndex().getName());
                }
                indexMetadata.getAliases().keysIt().forEachRemaining(duplicateAliasesIndices::add);
            }
            duplicateAliasesIndices.retainAll(allIndices);
            if (duplicateAliasesIndices.isEmpty() == false) {
                // iterate again and constructs a helpful message
                ArrayList<String> duplicates = new ArrayList<>();
                for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                    for (String alias: duplicateAliasesIndices) {
                        if (cursor.value.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.value.getIndex() + ")");
                        }
                    }
                }
                assert duplicates.size() > 0;
                throw new IllegalStateException("index and alias names need to be unique, but the following duplicates were found ["
                    + String.join(", ", duplicates) + "]");

            }

            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = Collections.unmodifiableSortedMap(buildAliasAndIndexLookup());


            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] allIndicesArray = allIndices.toArray(new String[allIndices.size()]);
            String[] allOpenIndicesArray = allOpenIndices.toArray(new String[allOpenIndices.size()]);
            String[] allClosedIndicesArray = allClosedIndices.toArray(new String[allClosedIndices.size()]);

            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                columnOidSupplier.columnOID,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                indices.build(),
                customs.build(),
                schemas.build(),
                allIndicesArray,
                allOpenIndicesArray,
                allClosedIndicesArray,
                aliasAndIndexLookup
            );
        }

        private SortedMap<String, AliasOrIndex> buildAliasAndIndexLookup() {
            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = new TreeMap<>();
            for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                IndexMetadata indexMetadata = cursor.value;
                AliasOrIndex existing = aliasAndIndexLookup.put(indexMetadata.getIndex().getName(), new AliasOrIndex.Index(indexMetadata));
                assert existing == null : "duplicate for " + indexMetadata.getIndex();

                for (ObjectObjectCursor<String, AliasMetadata> aliasCursor : indexMetadata.getAliases()) {
                    AliasMetadata aliasMetadata = aliasCursor.value;
                    aliasAndIndexLookup.compute(aliasMetadata.getAlias(), (aliasName, alias) -> {
                        if (alias == null) {
                            return new AliasOrIndex.Alias(aliasMetadata, indexMetadata);
                        } else {
                            assert alias instanceof AliasOrIndex.Alias : alias.getClass().getName();
                            ((AliasOrIndex.Alias) alias).addIndex(indexMetadata);
                            return alias;
                        }
                    });
                }
            }
            return aliasAndIndexLookup;
        }

        public static Metadata fromXContent(XContentParser parser, boolean preserveUnknownCustoms) throws IOException {
            Builder builder = new Builder();

            // we might get here after the meta-data element, or on a fresh parser
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (!"meta-data".equals(currentFieldName)) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    // move to the field name (meta-data)
                    token = parser.nextToken();
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new IllegalArgumentException("Expected a field name but got " + token);
                    }
                    // move to the next object
                    token = parser.nextToken();
                }
                currentFieldName = parser.currentName();
            }

            if (!"meta-data".equals(parser.currentName())) {
                throw new IllegalArgumentException("Expected [meta-data] as a field name but got " + currentFieldName);
            }
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected a START_OBJECT but got " + token);
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("cluster_coordination".equals(currentFieldName)) {
                        builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
                    } else if ("settings".equals(currentFieldName)) {
                        builder.persistentSettings(Settings.fromXContent(parser));
                    } else if ("indices".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetadata.Builder.fromXContent(parser), false);
                        }
                    } else {
                        try {
                            Custom custom = parser.namedObject(Custom.class, currentFieldName, null);
                            builder.putCustom(custom.getWriteableName(), custom);
                        } catch (NamedObjectNotFoundException ex) {
                            if (preserveUnknownCustoms) {
                                LOGGER.warn("Adding unknown custom object with type {}", currentFieldName);
                                builder.putCustom(currentFieldName, new UnknownGatewayOnlyCustom(parser.mapOrdered()));
                            } else {
                                LOGGER.warn("Skipping unknown custom object with type {}", currentFieldName);
                                parser.skipChildren();
                            }
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
                    } else if ("column_oid".equals(currentFieldName)) {
                        builder.columnOidSupplier = new ColumnOidSupplier(parser.longValue());
                    } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                        builder.clusterUUID = parser.text();
                    } else if ("cluster_uuid_committed".equals(currentFieldName)) {
                        builder.clusterUUIDCommitted = parser.booleanValue();
                    } else {
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            return builder.build();
        }

        public Builder dropTable(RelationName relationName) {
            SchemaMetadata schema = schemas.get(relationName.schema());
            if (schema == null) {
                return this;
            }
            ImmutableOpenMap<String, RelationMetadata> newRelations = ImmutableOpenMap.builder(schema.relations())
                .fRemove(relationName.name())
                .build();
            schemas.put(relationName.schema(), new SchemaMetadata(newRelations));
            return this;
        }

        public Builder addPartitions(RelationMetadata.Table table, ArrayList<String> newIndexUUIDs) {
            RelationName relationName = table.name();
            SchemaMetadata schema = schemas.get(relationName.schema());
            if (schema == null) {
                throw new IllegalStateException(String.format(
                    Locale.ENGLISH,
                    "Can't add partitions to table {}: SchemaMetadata is missing",
                    relationName
                ));
            }
            RelationMetadata.Table newTable = new RelationMetadata.Table(
                relationName,
                table.columns(),
                table.settings(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                newIndexUUIDs
            );
            ImmutableOpenMap<String, RelationMetadata> newRelations = ImmutableOpenMap.builder(schema.relations())
                .fPut(relationName.name(), newTable)
                .build();
            schemas.put(relationName.schema(), new SchemaMetadata(newRelations));
            return this;
        }

        @Nullable
        public RelationMetadata getRelation(RelationName relation) {
            SchemaMetadata schemaMetadata = schemas.get(relation.schema());
            if (schemaMetadata == null) {
                return null;
            }
            return schemaMetadata.relations().get(relation.name());
        }
    }

    public static class UnknownGatewayOnlyCustom implements Custom {

        private final Map<String, Object> contents;

        UnknownGatewayOnlyCustom(Map<String, Object> contents) {
            this.contents = contents;
        }

        @Override
        public EnumSet<XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Diff<Custom> diff(Custom previousState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Version getMinimalSupportedVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * State format for {@link Metadata} to write to and load from disk
     */

    public static final MetadataStateFormat<Metadata> FORMAT = createMetadataStateFormat(false);

    /**
     * Special state format for {@link Metadata} to write to and load from disk, preserving unknown customs
     */
    public static final MetadataStateFormat<Metadata> FORMAT_PRESERVE_CUSTOMS = createMetadataStateFormat(true);

    private static MetadataStateFormat<Metadata> createMetadataStateFormat(boolean preserveUnknownCustoms) {
        return new MetadataStateFormat<Metadata>(GLOBAL_STATE_FILE_PREFIX) {

            @Override
            public Metadata fromXContent(XContentParser parser) throws IOException {
                return Builder.fromXContent(parser, preserveUnknownCustoms);
            }

            @Override
            public Metadata readFrom(StreamInput in) throws IOException {
                return Metadata.readFrom(in);
            }
        };
    }

    public boolean contains(RelationName tableName) {
        if (getRelation(tableName) != null) {
            return true;
        }
        if (indices.containsKey(tableName.indexNameOrAlias())) {
            return true;
        }
        ViewsMetadata views = custom(ViewsMetadata.TYPE);
        if (views != null && views.contains(tableName)) {
            return true;
        }
        ForeignTablesMetadata foreignTables = custom(ForeignTablesMetadata.TYPE, ForeignTablesMetadata.EMPTY);
        if (foreignTables.contains(tableName)) {
            return true;
        }
        return false;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <T extends RelationMetadata> T getRelation(RelationName relation) {
        SchemaMetadata schemaMetadata = schemas.get(relation.schema());
        if (schemaMetadata == null) {
            return null;
        }
        RelationMetadata relationMetadata = schemaMetadata.get(relation);
        if (relationMetadata == null) {
            return null;
        }
        try {
            return (T) relationMetadata;
        } catch (ClassCastException e) {
            throw new OperationOnInaccessibleRelationException(
                relation,
                "The relation " + relation.sqlFqn() + " doesn't support the operation");
        }
    }

    public <T> List<T> getIndices(RelationName relationName,
                                  List<String> partitionValues,
                                  Function<IndexMetadata, T> as) {
        RelationMetadata relation = getRelation(relationName);
        if (!(relation instanceof RelationMetadata.Table table)) {
            return List.of();
        }
        ArrayList<T> result = new ArrayList<>();
        boolean targetsPartition = !partitionValues.isEmpty();
        for (String indexUUID : table.indexUUIDs()) {
            IndexMetadata indexMetadata = indexByUUID(indexUUID);
            if (indexMetadata == null) {
                // TODO: should this fail if not partitioned?
                continue;
            }
            if (!targetsPartition || indexMetadata.partitionValues().equals(partitionValues)) {
                T item = as.apply(indexMetadata);
                if (item != null) {
                    result.add(item);
                }
            }
        }
        return result;
    }

    @Nullable
    public RelationName getRelationName(String indexUUID) {
        // TODO: cache this into a uuid->name map when building the Metadata?
        for (var schemaCursor : schemas.values()) {
            SchemaMetadata schema = schemaCursor.value;
            for (var relationCursor : schema.relations().values()) {
                RelationMetadata relation = relationCursor.value;
                if (relation instanceof RelationMetadata.Table table) {
                    if (table.indexUUIDs().contains(indexUUID)) {
                        return table.name();
                    }
                }
            }
        }
        return null;
    }
}
