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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.cluster.NamedDiffableValueSerializer;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.rest.RestStatus;

public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata>, ToXContentFragment {

    private static final Logger LOGGER = LogManager.getLogger(Metadata.class);

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

    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment {

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

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final ImmutableOpenMap<String, IndexMetadata> indices;
    private final ImmutableOpenMap<String, IndexMetadata> indicesByUUID;
    private final ImmutableOpenMap<String, IndexTemplateMetadata> templates;
    private final ImmutableOpenMap<String, Custom> customs;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;
    private final int numberOfShards;

    private final String[] allIndices;
    private final String[] allOpenIndices;
    private final String[] allClosedIndices;

    private final SortedMap<String, AliasOrIndex> aliasAndIndexLookup;

    @SuppressWarnings("unchecked")
    Metadata(String clusterUUID,
             boolean clusterUUIDCommitted,
             long version,
             CoordinationMetadata coordinationMetadata,
             Settings transientSettings,
             Settings persistentSettings,
             ImmutableOpenMap<String, IndexMetadata> indices,
             ImmutableOpenMap<String, IndexTemplateMetadata> templates,
             ImmutableOpenMap<String, Custom> customs,
             String[] allIndices,
             String[] allOpenIndices,
             String[] allClosedIndices,
             SortedMap<String, AliasOrIndex> aliasAndIndexLookup) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.builder().put(persistentSettings).put(transientSettings).build();
        this.indices = indices;
        this.customs = customs;
        this.templates = templates;
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

    public boolean equalsAliases(Metadata other) {
        for (ObjectCursor<IndexMetadata> cursor : other.indices().values()) {
            IndexMetadata otherIndex = cursor.value;
            IndexMetadata thisIndex = index(otherIndex.getIndex());
            if (thisIndex == null) {
                return false;
            }
            if (otherIndex.getAliases().equals(thisIndex.getAliases()) == false) {
                return false;
            }
        }

        return true;
    }

    public SortedMap<String, AliasOrIndex> getAliasAndIndexLookup() {
        return aliasAndIndexLookup;
    }

    /**
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards and
     * that point to the specified concrete indices or match partially with the indices via wildcards.
     *
     * @param aliases         The names of the index aliases to find
     * @param concreteIndices The concrete indexes the index aliases must point to order to be returned.
     * @return a map of index to a list of alias metadata, the list corresponding to a concrete index will be empty if no aliases are
     * present for that index
     */
    public ImmutableOpenMap<String, List<AliasMetadata>> findAliases(final String[] aliases, String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        boolean matchAllAliases = matchAllAliases(aliases);
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder();
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = indices.get(index);
            List<AliasMetadata> filteredValues = new ArrayList<>();
            for (ObjectCursor<AliasMetadata> cursor : indexMetadata.getAliases().values()) {
                AliasMetadata value = cursor.value;
                if (matchAllAliases || Regex.simpleMatch(aliases, value.alias())) {
                    filteredValues.add(value);
                }
            }

            if (filteredValues.isEmpty() == false) {
                // Make the list order deterministic
                CollectionUtil.timSort(filteredValues, Comparator.comparing(AliasMetadata::alias));
                mapBuilder.put(index, Collections.unmodifiableList(filteredValues));
            }
        }
        return mapBuilder.build();
    }

    private static boolean matchAllAliases(final String[] aliases) {
        for (String alias : aliases) {
            if (alias.equals(ALL)) {
                return true;
            }
        }
        return aliases.length == 0;
    }

    /**
     * Checks if at least one of the specified aliases exists in the specified concrete indices. Wildcards are supported in the
     * alias names for partial matches.
     *
     * @param aliases         The names of the index aliases to find
     * @param concreteIndices The concrete indexes the index aliases must point to order to be returned.
     * @return whether at least one of the specified aliases exists in one of the specified concrete indices.
     */
    public boolean hasAliases(final String[] aliases, String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return false;
        }

        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        for (String index : intersection) {
            IndexMetadata indexMetadata = indices.get(index);
            List<AliasMetadata> filteredValues = new ArrayList<>();
            for (ObjectCursor<AliasMetadata> cursor : indexMetadata.getAliases().values()) {
                AliasMetadata value = cursor.value;
                if (Regex.simpleMatch(aliases, value.alias())) {
                    filteredValues.add(value);
                }
            }
            if (!filteredValues.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds all mappings for types and concrete indices. Types are expanded to include all types that match the glob
     * patterns in the types array. Empty types array, null or {"_all"} will be expanded to all types available for
     * the given indices. Only fields that match the provided field filter will be returned (default is a predicate
     * that always returns true, which can be overridden via plugins)
     *
     * @see MapperPlugin#getFieldFilter()
     *
     */
    public ImmutableOpenMap<String, MappingMetadata> findMappings(String[] concreteIndices,
                                                                  Function<String, Predicate<String>> fieldFilter) throws IOException {
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableOpenMap.of();
        }

        Iterable<String> intersection = HppcMaps.intersection(ObjectHashSet.from(concreteIndices), indices.keys());
        ImmutableOpenMap.Builder<String, MappingMetadata> indexMapBuilder = ImmutableOpenMap.builder();
        for (String index : intersection) {
            IndexMetadata indexMetadata = indices.get(index);
            Predicate<String> fieldPredicate = fieldFilter.apply(index);
            indexMapBuilder.put(index, filterFields(indexMetadata.mapping(), fieldPredicate));
        }
        return indexMapBuilder.build();
    }

    private static ImmutableOpenMap<String, MappingMetadata> filterFields(ImmutableOpenMap<String, MappingMetadata> mappings,
                                                                          Predicate<String> fieldPredicate) throws IOException {
        if (fieldPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
            return mappings;
        }
        ImmutableOpenMap.Builder<String, MappingMetadata> builder = ImmutableOpenMap.builder(mappings.size());
        for (ObjectObjectCursor<String, MappingMetadata> cursor : mappings) {
            builder.put(cursor.key, filterFields(cursor.value, fieldPredicate));
        }
        return builder.build(); // No types specified means return them all
    }

    @SuppressWarnings("unchecked")
    private static MappingMetadata filterFields(MappingMetadata mappingMetadata, Predicate<String> fieldPredicate) throws IOException {
        if (fieldPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
            return mappingMetadata;
        }
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(mappingMetadata.source().compressedReference(), true).v2();
        Map<String, Object> mapping;
        if (sourceAsMap.size() == 1 && sourceAsMap.containsKey(mappingMetadata.type())) {
            mapping = (Map<String, Object>) sourceAsMap.get(mappingMetadata.type());
        } else {
            mapping = sourceAsMap;
        }

        Map<String, Object> properties = (Map<String, Object>)mapping.get("properties");
        if (properties == null || properties.isEmpty()) {
            return mappingMetadata;
        }

        filterFields("", properties, fieldPredicate);

        return new MappingMetadata(mappingMetadata.type(), sourceAsMap);
    }

    @SuppressWarnings("unchecked")
    private static boolean filterFields(String currentPath, Map<String, Object> fields, Predicate<String> fieldPredicate) {
        assert fieldPredicate != MapperPlugin.NOOP_FIELD_PREDICATE;
        Iterator<Map.Entry<String, Object>> entryIterator = fields.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, Object> entry = entryIterator.next();
            String newPath = mergePaths(currentPath, entry.getKey());
            Object value = entry.getValue();
            boolean mayRemove = true;
            boolean isMultiField = false;
            if (value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                Map<String, Object> properties = (Map<String, Object>)map.get("properties");
                if (properties != null) {
                    mayRemove = filterFields(newPath, properties, fieldPredicate);
                } else {
                    Map<String, Object> subFields = (Map<String, Object>)map.get("fields");
                    if (subFields != null) {
                        isMultiField = true;
                        if (mayRemove = filterFields(newPath, subFields, fieldPredicate)) {
                            map.remove("fields");
                        }
                    }
                }
            } else {
                throw new IllegalStateException("cannot filter mappings, found unknown element of type [" + value.getClass() + "]");
            }

            //only remove a field if it has no sub-fields left and it has to be excluded
            if (fieldPredicate.test(newPath) == false) {
                if (mayRemove) {
                    entryIterator.remove();
                } else if (isMultiField) {
                    //multi fields that should be excluded but hold subfields that don't have to be excluded are converted to objects
                    Map<String, Object> map = (Map<String, Object>) value;
                    Map<String, Object> subFields = (Map<String, Object>)map.get("fields");
                    assert subFields.size() > 0;
                    map.put("properties", subFields);
                    map.remove("fields");
                    map.remove("type");
                }
            }
        }
        //return true if the ancestor may be removed, as it has no sub-fields left
        return fields.size() == 0;
    }

    private static String mergePaths(String path, String field) {
        if (path.length() == 0) {
            return field;
        }
        return path + "." + field;
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

    public boolean hasConcreteIndex(String index) {
        return getAliasAndIndexLookup().containsKey(index);
    }

    public IndexMetadata index(String index) {
        return indices.get(index);
    }

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

    public ImmutableOpenMap<String, IndexMetadata> indices() {
        return this.indices;
    }

    public ImmutableOpenMap<String, IndexMetadata> getIndices() {
        return indices();
    }

    public ImmutableOpenMap<String, IndexTemplateMetadata> templates() {
        return this.templates;
    }

    public ImmutableOpenMap<String, IndexTemplateMetadata> getTemplates() {
        return this.templates;
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
    }

    public ImmutableOpenMap<String, Custom> getCustoms() {
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

    /**
     * Identifies whether the array containing type names given as argument refers to all types
     * The empty or null array identifies all types
     *
     * @param types the array containing types
     * @return true if the provided array maps to all types, false otherwise
     */
    public static boolean isAllTypes(String[] types) {
        return types == null || types.length == 0 || isExplicitAllType(types);
    }

    /**
     * Identifies whether the array containing type names given as argument explicitly refers to all types
     * The empty or null array doesn't explicitly map to all types
     *
     * @param types the array containing index names
     * @return true if the provided array explicitly maps to all types, false otherwise
     */
    public static boolean isExplicitAllType(String[] types) {
        return types != null && types.length == 1 && ALL.equals(types[0]);
    }

    /**
     * @param concreteIndex The concrete index to check if routing is required
     * @param type          The type to check if routing is required
     * @return Whether routing is required according to the mapping for the specified index and type
     */
    public boolean routingRequired(String concreteIndex) {
        IndexMetadata indexMetadata = indices.get(concreteIndex);
        if (indexMetadata != null) {
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                return mappingMetadata.routing().required();
            }
        }
        return false;
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
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static class MetadataDiff implements Diff<Metadata> {

        private long version;

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private CoordinationMetadata coordinationMetadata;
        private Settings transientSettings;
        private Settings persistentSettings;
        private Diff<ImmutableOpenMap<String, IndexMetadata>> indices;
        private Diff<ImmutableOpenMap<String, IndexTemplateMetadata>> templates;
        private Diff<ImmutableOpenMap<String, Custom>> customs;

        MetadataDiff(Metadata before, Metadata after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
            templates = DiffableUtils.diff(before.templates, after.templates, DiffableUtils.getStringKeySerializer());
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        MetadataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            indices = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), IndexMetadata::readFrom,
                IndexMetadata::readDiffFrom);
            templates = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), IndexTemplateMetadata::readFrom,
                IndexTemplateMetadata::readDiffFrom);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetadata.writeTo(out);
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public Metadata apply(Metadata part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.indices(indices.apply(part.indices));
            builder.templates(templates.apply(part.templates));
            builder.customs(customs.apply(part.customs));
            return builder.build();
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in), false);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexTemplateMetadata.readFrom(in));
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        writeSettingsToStream(transientSettings, out);
        writeSettingsToStream(persistentSettings, out);
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out);
        }
        out.writeVInt(templates.size());
        for (ObjectCursor<IndexTemplateMetadata> cursor : templates.values()) {
            cursor.value.writeTo(out);
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
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Metadata metadata) {
        return new Builder(metadata);
    }

    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;

        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = Settings.Builder.EMPTY_SETTINGS;

        private final ImmutableOpenMap.Builder<String, IndexMetadata> indices;
        private final ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templates;
        private final ImmutableOpenMap.Builder<String, Custom> customs;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = ImmutableOpenMap.builder();
            templates = ImmutableOpenMap.builder();
            customs = ImmutableOpenMap.builder();
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.version = metadata.version;
            this.indices = ImmutableOpenMap.builder(metadata.indices);
            this.templates = ImmutableOpenMap.builder(metadata.templates);
            this.customs = ImmutableOpenMap.builder(metadata.customs);
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

        public Builder put(IndexTemplateMetadata.Builder template) {
            return put(template.build());
        }

        public Builder put(IndexTemplateMetadata template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

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
            @SuppressWarnings("unchecked") IndexGraveyard graveyard = (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
            return graveyard;
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keys().toArray(String.class);
            }
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata)
                        .settings(Settings.builder().put(indexMetadata.getSettings()).put(settings)));
            }
            return this;
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
                    + Strings.collectionToCommaDelimitedString(duplicates) + "]");

            }

            SortedMap<String, AliasOrIndex> aliasAndIndexLookup = Collections.unmodifiableSortedMap(buildAliasAndIndexLookup());


            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] allIndicesArray = allIndices.toArray(new String[allIndices.size()]);
            String[] allOpenIndicesArray = allOpenIndices.toArray(new String[allOpenIndices.size()]);
            String[] allClosedIndicesArray = allClosedIndices.toArray(new String[allClosedIndices.size()]);

            return new Metadata(clusterUUID, clusterUUIDCommitted, version, coordinationMetadata, transientSettings, persistentSettings,
                                indices.build(), templates.build(), customs.build(), allIndicesArray, allOpenIndicesArray, allClosedIndicesArray,
                                aliasAndIndexLookup);
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
            aliasAndIndexLookup.values().stream().filter(AliasOrIndex::isAlias)
                .forEach(alias -> ((AliasOrIndex.Alias) alias).computeAndValidateWriteIndex());
            return aliasAndIndexLookup;
        }

        public static String toXContent(Metadata metadata) throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            toXContent(metadata, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }

        public static void toXContent(Metadata metadata, XContentBuilder builder, ToXContent.Params params) throws IOException {

            builder.startObject("meta-data");

            builder.field("version", metadata.version());
            builder.field("cluster_uuid", metadata.clusterUUID);
            builder.field("cluster_uuid_committed", metadata.clusterUUIDCommitted);

            builder.startObject("cluster_coordination");
            metadata.coordinationMetadata().toXContent(builder, params);
            builder.endObject();

            if (!metadata.persistentSettings().isEmpty()) {
                builder.startObject("settings");
                metadata.persistentSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, "API"));
            if (context == XContentContext.API && !metadata.transientSettings().isEmpty()) {
                builder.startObject("transient_settings");
                metadata.transientSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            builder.startObject("templates");
            for (ObjectCursor<IndexTemplateMetadata> cursor : metadata.templates().values()) {
                IndexTemplateMetadata.Builder.toXContent(cursor.value, builder, params);
            }
            builder.endObject();

            if (context == XContentContext.API && !metadata.indices().isEmpty()) {
                builder.startObject("indices");
                for (IndexMetadata indexMetadata : metadata) {
                    IndexMetadata.Builder.toXContent(indexMetadata, builder, params);
                }
                builder.endObject();
            }

            for (ObjectObjectCursor<String, Custom> cursor : metadata.customs()) {
                if (cursor.value.context().contains(context)) {
                    builder.startObject(cursor.key);
                    cursor.value.toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }

        public static Metadata fromXContent(XContentParser parser) throws IOException {
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
                            LOGGER.warn("Skipping unknown custom object with type {}", currentFieldName);
                            parser.skipChildren();
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
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
    }

    private static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

    /**
     * State format for {@link Metadata} to write to and load from disk
     */
    public static final MetadataStateFormat<Metadata> FORMAT = new MetadataStateFormat<Metadata>(GLOBAL_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, Metadata state) throws IOException {
            Builder.toXContent(state, builder, FORMAT_PARAMS);
        }

        @Override
        public Metadata fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };
}
