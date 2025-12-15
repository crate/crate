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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.Diffs;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.hppc.procedures.ObjectProcedure;

import io.crate.common.collections.Lists;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.ddl.Templates;
import io.crate.expression.symbol.RefReplacer;
import io.crate.fdw.ForeignTablesMetadata;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.rest.action.HttpErrorStatus;
import io.crate.sql.tree.ColumnPolicy;

public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata> {

    private static final Logger LOGGER = LogManager.getLogger(Metadata.class);
    public static final long COLUMN_OID_UNASSIGNED = 0L;

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
     * Indicates that this custom metadata will be returned as part of an API call and will be persisted between
     * node restarts, but will not be a part of a snapshot global state
     */
    static final EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);

    public interface Custom extends NamedDiffable<Custom> {

        EnumSet<XContentContext> context();
    }

    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING =
        Setting.boolSetting("cluster.blocks.read_only", false, Property.Dynamic, Property.NodeScope);

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(6, "cluster read-only (api)", false, false,
        false, HttpErrorStatus.RELATION_READ_ONLY, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING =
        Setting.boolSetting("cluster.blocks.read_only_allow_delete", false, Property.Dynamic, Property.NodeScope);

    public static final ClusterBlock CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(13, "cluster read-only / allow delete (api)",
        false, false, true, HttpErrorStatus.RELATION_READ_DELETE_ONLY, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    public static final Metadata EMPTY_METADATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;
    @Deprecated
    private final long columnOID;

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final ImmutableOpenMap<String, IndexMetadata> indices;
    private final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    private final ImmutableOpenMap<String, Custom> customs;
    private final ImmutableOpenMap<String, SchemaMetadata> schemas;

    private final transient ImmutableOpenMap<String, RelationMetadata> indexUUIDsRelations;
    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;
    private final int numberOfShards;

    private final SortedMap<String, AliasOrIndex> aliasAndIndexLookup;

    Metadata(String clusterUUID,
             boolean clusterUUIDCommitted,
             long version,
             long columnOID,
             CoordinationMetadata coordinationMetadata,
             Settings transientSettings,
             Settings persistentSettings,
             ImmutableOpenMap<String, IndexMetadata> indices,
             ImmutableOpenMap<String, IndexTemplateMetadata> templates,
             ImmutableOpenMap<String, Custom> customs,
             ImmutableOpenMap<String, SchemaMetadata> schemas,
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
        this.templates = templates;
        int totalNumberOfShards = 0;
        int totalOpenIndexShards = 0;
        int numberOfShards = 0;
        for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
            var indexMetadata = cursor.value;
            totalNumberOfShards += indexMetadata.getTotalNumberOfShards();
            numberOfShards += indexMetadata.getNumberOfShards();
            if (IndexMetadata.State.OPEN.equals(indexMetadata.getState())) {
                totalOpenIndexShards += indexMetadata.getTotalNumberOfShards();
            }
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenIndexShards = totalOpenIndexShards;
        this.numberOfShards = numberOfShards;
        this.aliasAndIndexLookup = aliasAndIndexLookup;
        var indexUUIDsRelationsBuilder = ImmutableOpenMap.<String, RelationMetadata>builder(indices.size());
        for (var cursor : schemas) {
            SchemaMetadata schema = cursor.value;
            for (var relCursor : schema.relations()) {
                var relationMetadata = relCursor.value;
                for (String indexUUID : relationMetadata.indexUUIDs()) {
                    RelationMetadata old = indexUUIDsRelationsBuilder.put(indexUUID, relationMetadata);
                    assert old == null : "A index must not be referenced from multiple relations";
                }
            }
        }
        indexUUIDsRelations = indexUUIDsRelationsBuilder.build();
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

    public boolean hasIndex(String indexUUID) {
        return indices.containsKey(indexUUID);
    }

    public boolean hasIndex(Index index) {
        return indices.containsKey(index.getUUID());
    }

    public boolean hasConcreteIndex(String indexUUID) {
        return getAliasAndIndexLookup().containsKey(indexUUID);
    }

    @Nullable
    public IndexMetadata index(String indexUUID) {
        return indices.get(indexUUID);
    }

    @Nullable
    public IndexMetadata index(Index index) {
        return indices.get(index.getUUID());
    }

    /**
     * Returns true iff existing index has the same {@link IndexMetadata} instance
     */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getUUID()) == indexMetadata;
    }

    /**
     * Returns the {@link IndexMetadata} for this index.
     *
     * @throws IndexNotFoundException if no metadata for this index is found
     */
    public IndexMetadata getIndexSafe(Index index) {
        IndexMetadata metadata = index(index);
        if (metadata != null) {
            return metadata;
        }
        throw new IndexNotFoundException(index);
    }

    /// Lookup [IndexMetadata][IndexMetadata] by name. This variant should be avoided.
    /// Use [index(Index)][#index(Index)] or [index(String)][#index(String)] instead.
    @Nullable
    public IndexMetadata getIndexByName(String indexName) {
        for (var cursor : indices.values()) {
            IndexMetadata indexMetadata = cursor.value;
            if (indexMetadata.getIndex().getName().equals(indexName)) {
                return indexMetadata;
            }
        }
        return null;
    }

    /**
     * @return indexName -> indexMetadata
     **/
    public ImmutableOpenMap<String, IndexMetadata> indices() {
        return this.indices;
    }

    @Deprecated
    public ImmutableOpenMap<String, IndexTemplateMetadata> templates() {
        return this.templates;
    }

    public ImmutableOpenMap<String, SchemaMetadata> schemas() {
        return this.schemas;
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
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
     *
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     *
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return this.totalOpenIndexShards;
    }

    /**
     * Gets the number of primary shards from all indices, not including
     * replicas.
     *
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
        if (!metadata1.templates.equals(metadata2.templates())) {
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
        if (!metadata1.schemas.equals(metadata2.schemas)) {
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
    public Diff<Metadata> diff(Version version, Metadata previousState) {
        return new MetadataDiff(version, previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        return new MetadataDiff(in);
    }

    public static Metadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser, false);
    }

    private static boolean hasGlobalColumnOID(Version version) {
        return version.onOrAfter(Version.V_5_5_0) &&
            (version.onOrBefore(Version.V_6_0_3) || version.equals(Version.V_6_1_0));
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
        private final Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates;
        private final Diff<ImmutableOpenMap<String, Custom>> customs;
        private final Diff<ImmutableOpenMap<String, SchemaMetadata>> schemas;

        MetadataDiff(Version v, Metadata before, Metadata after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            columnOID = after.columnOID;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            indices = Diffs.diff(v, before.indices, after.indices, Diffs.stringKeySerializer());
            templates = Diffs.diff(v, before.templates, after.templates, Diffs.stringKeySerializer());
            customs = Diffs.diff(v, before.customs, after.customs, Diffs.stringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            schemas = Diffs.diff(v, before.schemas, after.schemas, Diffs.stringKeySerializer());
        }

        private static final Diffs.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final Diffs.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);
        private static final Diffs.DiffableValueReader<String, SchemaMetadata> SCHEMA_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(SchemaMetadata::of, SchemaMetadata::readDiffFrom);

        MetadataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            if (hasGlobalColumnOID(in.getVersion())) {
                columnOID = in.readLong();
            } else {
                columnOID = COLUMN_OID_UNASSIGNED;
            }
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            indices = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            customs = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            if (in.getVersion().onOrAfter(Version.V_6_0_0)) {
                schemas = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), SCHEMA_DIFF_VALUE_READER);
            } else {
                schemas = Diffs.diff(
                    in.getVersion(),
                    ImmutableOpenMap.<String, SchemaMetadata>of(),
                    ImmutableOpenMap.<String, SchemaMetadata>of(),
                    Diffs.stringKeySerializer()
                );
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            if (hasGlobalColumnOID(out.getVersion())) {
                out.writeLong(columnOID);
            }
            coordinationMetadata.writeTo(out);
            Settings.writeSettingsToStream(out, transientSettings);
            Settings.writeSettingsToStream(out, persistentSettings);
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
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
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            builder.schemas.putAll(schemas.apply(part.schemas));
            return builder.build();
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        if (hasGlobalColumnOID(in.getVersion())) {
            builder.columnOID(in.readLong());
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
        // Only read templates if we are not on 6.0.0 or later, templates aren't used anymore. But old ones must still
        // be read to maintain backwards compatibility. They will be converted to schemas/relations and removed
        // afterward in the PublicationTransportHandler
        if (in.getVersion().before(Version.V_6_0_0)) {
            int templatesSize = in.readVInt();
            for (int i = 0; i < templatesSize; i++) {
                builder.put(IndexTemplateMetadata.readFrom(in));
            }
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }

        if (in.getVersion().onOrAfter(Version.V_6_0_0)) {
            int numSchemas = in.readVInt();
            for (int i = 0; i < numSchemas; i++) {
                String schemaName = in.readString();
                SchemaMetadata schemaMetadata = SchemaMetadata.of(in);
                builder.put(schemaName, schemaMetadata);
            }
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        if (hasGlobalColumnOID(out.getVersion())) {
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
        if (out.getVersion().before(Version.V_6_0_0)) {
            List<RelationMetadata.Table> partitionedRelations = relations(RelationMetadata.Table.class).stream()
                .filter(table -> table.partitionedBy().isEmpty() == false)
                .toList();
            out.writeVInt(partitionedRelations.size());
            for (RelationMetadata.Table table : partitionedRelations) {
                IndexTemplateMetadata templateMetadata = Templates.of(table);
                templateMetadata.writeTo(out);
            }
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

        if (out.getVersion().onOrAfter(Version.V_6_0_0)) {
            out.writeVInt(schemas.size());
            for (var cursor : schemas) {
                String schemaName = cursor.key;
                SchemaMetadata schema = cursor.value;

                out.writeString(schemaName);
                schema.writeTo(out);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Metadata metadata) {
        return new Builder(metadata);
    }

    public static class Builder {

        public static final LongSupplier NO_OID_COLUMN_OID_SUPPLIER = () -> COLUMN_OID_UNASSIGNED;
        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;
        private long columnOID;
        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.EMPTY;
        private Settings persistentSettings = Settings.EMPTY;

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;
        private final ImmutableOpenMap.Builder<String, SchemaMetadata> schemas;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            schemas = ImmutableOpenMap.builder();
            columnOID = COLUMN_OID_UNASSIGNED;
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.version = metadata.version;
            this.columnOID = metadata.columnOID;
            this.indices = ImmutableOpenMap.builder(metadata.indices);
            this.templates = ImmutableOpenMap.builder(metadata.templates);
            this.customs = ImmutableOpenMap.builder(metadata.customs);
            this.schemas = ImmutableOpenMap.builder(metadata.schemas);
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            indices.put(indexMetadata.getIndex().getUUID(), indexMetadata);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            if (indices.get(indexMetadata.getIndex().getUUID()) == indexMetadata) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetadata = IndexMetadata.builder(indexMetadata).version(indexMetadata.getVersion() + 1).build();
            }
            indices.put(indexMetadata.getIndex().getUUID(), indexMetadata);
            return this;
        }

        public Builder putWithIndexName(IndexMetadata indexMetadata) {
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public IndexMetadata get(String indexUUID) {
            return indices.get(indexUUID);
        }

        public IndexMetadata getSafe(Index index) {
            IndexMetadata indexMetadata = get(index.getUUID());
            if (indexMetadata != null) {
                return indexMetadata;
            }
            throw new IndexNotFoundException(index);
        }

        public Builder remove(String indexUUID) {
            indices.remove(indexUUID);
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

        // Still required for BWC, can be removed once rolling upgrade from 5.x isn't supported anymore
        @Deprecated
        public Builder put(IndexTemplateMetadata template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder put(String schemaName, SchemaMetadata schemaMetadata) {
            schemas.put(schemaName, schemaMetadata);
            return this;
        }

        public Builder dropRelation(RelationName relationName) {
            SchemaMetadata schemaMetadata = schemas.get(relationName.schema());
            if (schemaMetadata == null) {
                return this;
            }
            ImmutableOpenMap<String, RelationMetadata> newRelations = ImmutableOpenMap.builder(schemaMetadata.relations())
                .fRemove(relationName.name())
                .build();
            if (newRelations.isEmpty() && !schemaMetadata.explicit()) {
                schemas.remove(relationName.schema());
            } else {
                schemas.put(relationName.schema(), new SchemaMetadata(newRelations, schemaMetadata.explicit()));
            }
            return this;
        }

        @Nullable
        public <T extends RelationMetadata> T getRelation(RelationName relation) {
            return Metadata.getRelation(relation, schemas::get);
        }

        public Builder setBlobTable(RelationName name, String indexUUID, Settings settings, State state) {
            setRelation(new RelationMetadata.BlobTable(name, indexUUID, settings, state));
            return this;
        }

        /**
         * Adds the relation to the corresponding {@link SchemaMetadata}.
         * If the relation already exists with the same name it is overridden.
         **/
        public void setRelation(RelationMetadata relation) {
            ImmutableOpenMap<String, RelationMetadata> relations;
            RelationName relationName = relation.name();
            String schema = relationName.schema();
            SchemaMetadata schemaMetadata = schemas.get(schema);
            boolean explicit;
            if (schemaMetadata == null) {
                relations = ImmutableOpenMap.<String, RelationMetadata>builder(1)
                    .fPut(relationName.name(), relation)
                    .build();
                explicit = false;
            } else {
                relations = ImmutableOpenMap.builder(schemaMetadata.relations())
                    .fPut(relationName.name(), relation)
                    .build();
                explicit = schemaMetadata.explicit();
            }
            schemas.put(schema, new SchemaMetadata(relations, explicit));
        }

        @Deprecated
        public IndexTemplateMetadata getTemplate(String templateName) {
            return templates.get(templateName);
        }

        @Deprecated
        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        @Deprecated
        public Builder templates(ImmutableOpenMap<String, IndexTemplateMetadata> templates) {
            this.templates.putAll(templates);
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
         * @param indicesUUIDs     the indices to update the number of replicas for
         * @return the builder
         */
        public Builder updateNumberOfReplicas(final int numberOfReplicas, final String[] indicesUUIDs) {
            for (String indexUUID : indicesUUIDs) {
                IndexMetadata indexMetadata = this.indices.get(indexUUID);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(indexUUID);
                }
                put(IndexMetadata.builder(indexMetadata).numberOfReplicas(numberOfReplicas));
            }
            return this;
        }

        public void updateNumberOfReplicas(final int numberOfReplicas, List<IndexMetadata> indexes) {
            for (IndexMetadata im : indexes) {
                put(IndexMetadata.builder(im).numberOfReplicas(numberOfReplicas));
            }
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

        @Deprecated
        public Builder columnOID(long columnOID) {
            this.columnOID = columnOID;
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

        public Metadata build() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will only be rebuilded when needed. Now during serializing we rebuild these datastructures
            //    while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            final Set<String> allIndices = new HashSet<>(indices.size());
            final Set<String> duplicateAliasesIndices = new HashSet<>();
            for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                final IndexMetadata indexMetadata = cursor.value;
                final String uuid = indexMetadata.getIndex().getUUID();
                boolean added = allIndices.add(uuid);
                assert added : "double index named [" + uuid + "]";
                indexMetadata.getAliases().keysIt().forEachRemaining(duplicateAliasesIndices::add);
            }
            duplicateAliasesIndices.retainAll(allIndices);
            if (duplicateAliasesIndices.isEmpty() == false) {
                // iterate again and constructs a helpful message
                ArrayList<String> duplicates = new ArrayList<>();
                for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                    for (String alias : duplicateAliasesIndices) {
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

            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                columnOID,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                indices.build(),
                templates.build(),
                customs.build(),
                schemas.build(),
                aliasAndIndexLookup
            );
        }

        private SortedMap<String, AliasOrIndex> buildAliasAndIndexLookup() {
            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = new TreeMap<>();
            for (ObjectCursor<IndexMetadata> cursor : indices.values()) {
                IndexMetadata indexMetadata = cursor.value;
                if (indexMetadata.getCreationVersion().onOrAfter(Version.V_6_0_0)) {
                    // aliases are deprecated and only needed to be built for old indices, aliases will be removed once
                    // the metadata is fully migrated/upgraded to schemas/relations.
                    continue;
                }

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
                    } else if ("templates".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName()));
                        }
                    } else {
                        try {
                            Custom custom = parser.namedObject(Custom.class, currentFieldName, null);
                            builder.putCustom(custom.getWriteableName(), custom);
                        } catch (NamedObjectNotFoundException ex) {
                            if (preserveUnknownCustoms) {
                                LOGGER.warn("Adding unknown custom object with type {}", currentFieldName);
                                parser.mapOrdered();
                                builder.putCustom(currentFieldName, new UnknownGatewayOnlyCustom());
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
                        builder.columnOID = parser.longValue();
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

        /**
         * <p>
         * Adds the table, overriding it if a table with the same schema and name exists.
         * </p>
         * <p>
         * oidSupplier is only parameterized for testing.
         * For production code use {@link #setTable(RelationName, List, Settings, ColumnIdent, ColumnPolicy, String, Map, List, List, State, List, long)
         **/
        @VisibleForTesting
        public Builder setTable(LongSupplier oidSupplier,
                                RelationName relationName,
                                List<Reference> columns,
                                Settings settings,
                                @Nullable ColumnIdent routingColumn,
                                ColumnPolicy columnPolicy,
                                @Nullable String pkConstraintName,
                                Map<String, String> checkConstraints,
                                List<ColumnIdent> primaryKeys,
                                List<ColumnIdent> partitionedBy,
                                State state,
                                List<String> indexUUIDs,
                                long tableVersion) {
            AtomicInteger positions = new AtomicInteger(0);
            Map<ColumnIdent, Reference> columnMap = columns.stream()
                .filter(ref -> !ref.isDropped())
                .map(ref -> ref.withOidAndPosition(oidSupplier, positions::incrementAndGet))
                .collect(Collectors.toMap(ref -> ref.column(), ref -> ref));

            ArrayList<Reference> finalColumns = new ArrayList<>(columns.size());
            // Need to update linked columns for generated and index refs to ensure these have oid+position
            // Otherwise .contains/.equals on those refs is broken.
            for (var column : columnMap.values()) {
                Reference newRef;
                if (column instanceof GeneratedReference genRef) {
                    newRef = new GeneratedReference(
                        genRef.reference(),
                        RefReplacer.replaceRefs(genRef.generatedExpression(), ref -> columnMap.get(ref.column()))
                    );
                } else if (column instanceof IndexReference indexRef) {
                    List<Reference> newColumns = Lists.map(indexRef.columns(), x -> Objects.requireNonNull(columnMap.get(x.column())));
                    newRef = indexRef.withColumns(newColumns);
                } else {
                    newRef = column;
                }
                finalColumns.add(newRef);
            }
            columns.stream()
                .filter(Reference::isDropped)
                .forEach(ref -> finalColumns.add(ref));
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
                indexUUIDs,
                tableVersion
            );
            setRelation(table);
            return this;
        }

        /**
         * Adds the table, overriding it if a table with the same schema and name exists.
         **/
        public Builder setTable(RelationName relationName,
                                List<Reference> columns,
                                Settings settings,
                                @Nullable ColumnIdent routingColumn,
                                ColumnPolicy columnPolicy,
                                @Nullable String pkConstraintName,
                                Map<String, String> checkConstraints,
                                List<ColumnIdent> primaryKeys,
                                List<ColumnIdent> partitionedBy,
                                State state,
                                List<String> indexUUIDs,
                                long tableVersion) {
            return setTable(
                new DocTableInfo.OidSupplier(0),
                relationName,
                columns,
                settings,
                routingColumn,
                columnPolicy,
                pkConstraintName,
                checkConstraints,
                primaryKeys,
                partitionedBy,
                state,
                indexUUIDs,
                tableVersion
            );
        }

        public Builder addIndexUUIDs(RelationMetadata.Table table, List<String> indexUUIDs) {
            RelationMetadata.Table updatedTable = new RelationMetadata.Table(
                table.name(),
                table.columns(),
                table.settings(),
                table.routingColumn(),
                table.columnPolicy(),
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                Lists.concat(table.indexUUIDs(), indexUUIDs),
                table.tableVersion() + 1
            );
            setRelation(updatedTable);
            return this;
        }

        /// Explicitly adds a schema
        public Builder createSchema(String schemaName) {
            schemas.put(schemaName, new SchemaMetadata(ImmutableOpenMap.of(), true));
            return this;
        }

        public Builder dropSchema(String schema) {
            schemas.remove(schema);
            return this;
        }
    }

    public static class UnknownGatewayOnlyCustom implements Custom {

        UnknownGatewayOnlyCustom() {
        }

        @Override
        public EnumSet<XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Diff<Custom> diff(Version version, Custom previousState) {
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
        return new MetadataStateFormat<>(GLOBAL_STATE_FILE_PREFIX) {

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
        if (templates.containsKey(PartitionName.templateName(tableName.schema(), tableName.name()))) {
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
        SchemaMetadata schemaMetadata = schemas.get(tableName.schema());
        if (schemaMetadata != null && schemaMetadata.relations().containsKey(tableName.name())) {
            return true;
        }
        return getRelation(tableName) != null;
    }

    @Nullable
    public <T extends RelationMetadata> T getRelation(RelationName relation) {
        return getRelation(relation, schemas::get);
    }

    @Nullable
    public RelationMetadata getRelation(String indexUUID) {
        return indexUUIDsRelations.get(indexUUID);
    }

    /**
     * @throws RelationUnknown
     * @throws IndexNotFoundException
     **/
    public PartitionName getPartitionName(String indexUUID) {
        RelationMetadata relationMetadata = getRelation(indexUUID);
        if (relationMetadata == null) {
            throw new RelationUnknown(
                String.format(Locale.ENGLISH, "Relation not found for indexUUID=%s", indexUUID));
        }
        return getPartitionName(relationMetadata.name(), indexUUID);
    }

    /**
     * @throws IndexNotFoundException
     **/
    public PartitionName getPartitionName(RelationName relationName, String indexUUID) {
        IndexMetadata indexMetadata = index(indexUUID);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(
                String.format(Locale.ENGLISH, "Index metadata not found for indexUUID=%s", indexUUID));
        }
        return new PartitionName(relationName, indexMetadata.partitionValues());
    }

    public <T extends RelationMetadata> List<T> relations(Class<T> clazz) {
        return relations(clazz::isInstance, clazz::cast);
    }

    public <T extends RelationMetadata> List<T> relations(String schemaName, Class<T> clazz) {
        SchemaMetadata schemaMetadata = schemas.get(schemaName);
        if (schemaMetadata == null) {
            return List.of();
        }
        ArrayList<T> relations = new ArrayList<>();
        for (ObjectCursor<RelationMetadata> relationCursor : schemaMetadata.relations().values()) {
            RelationMetadata relationMetadata = relationCursor.value;
            if (clazz.isInstance(relationMetadata)) {
                relations.add(clazz.cast(relationMetadata));
            }
        }
        return relations;
    }

    public <T> List<T> relations(Predicate<RelationMetadata> predicate, Function<RelationMetadata, T> as) {
        ArrayList<T> relations = new ArrayList<>();
        for (ObjectCursor<SchemaMetadata> cursor : schemas.values()) {
            for (ObjectCursor<RelationMetadata> relationCursor : cursor.value.relations().values()) {
                if (predicate.test(relationCursor.value)) {
                    relations.add(as.apply(relationCursor.value));
                }
            }
        }
        return relations;
    }

    /**
     * <p>
     * Resolve the indices for a list of partitions and return their data either as
     * {@link IndexMetadata} or derived from it using the {@code as} parameter.
     * </p>
     * <p>
     * An empty list will resolve all indices in the cluster, open and closed
     * </p>
     * <p>
     * {@code null} values returned from {@code as} are excluded from the result.
     * This can be used to filter based on state or similar.
     * </p>
     *
     * @param partitions A list of partitions to resolve
     **/
    public <T> List<T> getIndices(List<PartitionName> partitions, boolean strict, Function<IndexMetadata, T> as) {
        if (partitions.isEmpty()) {
            List<T> allIndices = new ArrayList<>();
            indices.values().forEach((ObjectProcedure<IndexMetadata>) value -> allIndices.add(as.apply(value)));
            return allIndices;
        }
        List<T> result = new ArrayList<>();
        for (PartitionName r : partitions) {
            result.addAll(getIndices(r.relationName(), r.values(), strict, as));
        }
        return result;
    }

    /**
     * <p>
     * Resolve the indices for a relation and return their data either as
     * {@link IndexMetadata} or derived from it using the {@code as} parameter.
     * </p>
     * <p>
     * {@code null} values returned from {@code as} are excluded from the result.
     * This can be used to filter based on state or similar.
     * </p>
     *
     * @param partitionValues filter by a single partition. Use `List.of()` to include all partitions.
     **/
    public <T> List<T> getIndices(RelationName relationName,
                                  List<String> partitionValues,
                                  boolean strict,
                                  Function<IndexMetadata, T> as) {
        RelationMetadata relation = getRelation(relationName);
        switch (relation) {
            case null -> {
                if (strict) {
                    throw new RelationUnknown(relationName);
                }
                return List.of();
            }
            case RelationMetadata.BlobTable blobTable -> {
                IndexMetadata imd = index(blobTable.indexUUID());
                if (imd == null) {
                    throw new RelationUnknown(relationName);
                }
                T item = as.apply(imd);
                if (item != null) {
                    return List.of(item);
                }
                return List.of();
            }
            case RelationMetadata.Table table -> {
                List<String> indexUUIDs = table.indexUUIDs();
                ArrayList<T> result = new ArrayList<>(indexUUIDs.size());
                for (String indexUUID : indexUUIDs) {
                    IndexMetadata imd = index(indexUUID);
                    if (imd == null) {
                        if (strict) {
                            throw new RelationUnknown(relationName);
                        }
                        continue;
                    }
                    if (!partitionValues.isEmpty() && !partitionValues.equals(imd.partitionValues())) {
                        continue;
                    }
                    T item = as.apply(imd);
                    if (item != null) {
                        result.add(item);
                    }
                }
                return result;
            }
            default -> {
            }
        }
        // should be never reached
        throw new UnsupportedOperationException("Unsupported relation type: " + relation.getClass().getName());
    }

    @Nullable
    public <T> T getIndex(RelationName relationName,
                          List<String> partitionValues,
                          boolean strict,
                          Function<IndexMetadata, T> as) {
        List<T> indices = getIndices(relationName, partitionValues, strict, as);
        if (indices.size() > 1) {
            throw new IllegalArgumentException("Expected a single index for " + relationName + " but got " + indices.size());
        } else if (indices.size() == 1) {
            return indices.getFirst();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends RelationMetadata> T getRelation(RelationName relation, Function<String, SchemaMetadata> schemaResolver) {
        SchemaMetadata schemaMetadata = schemaResolver.apply(relation.schema());
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
}
