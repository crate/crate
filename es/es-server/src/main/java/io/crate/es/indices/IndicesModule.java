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

package io.crate.es.indices;

import io.crate.es.action.resync.TransportResyncReplicationAction;
import io.crate.es.common.geo.ShapesAvailability;
import io.crate.es.common.inject.AbstractModule;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.io.stream.NamedWriteableRegistry.Entry;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.index.IndexSettings;
import io.crate.es.index.engine.EngineFactory;
import io.crate.es.index.mapper.BinaryFieldMapper;
import io.crate.es.index.mapper.BooleanFieldMapper;
import io.crate.es.index.mapper.DateFieldMapper;
import io.crate.es.index.mapper.FieldAliasMapper;
import io.crate.es.index.mapper.FieldNamesFieldMapper;
import io.crate.es.index.mapper.GeoPointFieldMapper;
import io.crate.es.index.mapper.GeoShapeFieldMapper;
import io.crate.es.index.mapper.IdFieldMapper;
import io.crate.es.index.mapper.IgnoredFieldMapper;
import io.crate.es.index.mapper.IpFieldMapper;
import io.crate.es.index.mapper.KeywordFieldMapper;
import io.crate.es.index.mapper.Mapper;
import io.crate.es.index.mapper.MetadataFieldMapper;
import io.crate.es.index.mapper.NumberFieldMapper;
import io.crate.es.index.mapper.ObjectMapper;
import io.crate.es.index.mapper.RoutingFieldMapper;
import io.crate.es.index.mapper.SeqNoFieldMapper;
import io.crate.es.index.mapper.SourceFieldMapper;
import io.crate.es.index.mapper.TextFieldMapper;
import io.crate.es.index.mapper.TypeFieldMapper;
import io.crate.es.index.mapper.UidFieldMapper;
import io.crate.es.index.mapper.VersionFieldMapper;
import io.crate.es.index.seqno.GlobalCheckpointSyncAction;
import io.crate.es.index.shard.PrimaryReplicaSyncer;
import io.crate.es.indices.cluster.IndicesClusterStateService;
import io.crate.es.indices.flush.SyncedFlushService;
import io.crate.es.indices.mapper.MapperRegistry;
import io.crate.es.indices.store.IndicesStore;
import io.crate.es.indices.store.TransportNodesListShardStoreMetaData;
import io.crate.es.plugins.MapperPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {
    private final List<Entry> namedWritables = new ArrayList<>();
    private final MapperRegistry mapperRegistry;

    public IndicesModule(List<MapperPlugin> mapperPlugins) {
        this.mapperRegistry = new MapperRegistry(getMappers(mapperPlugins), getMetadataMappers(mapperPlugins),
                getFieldFilter(mapperPlugins));
        registerBuiltinWritables();
    }

    private void registerBuiltinWritables() {
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return namedWritables;
    }

    public List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Collections.emptyList();
    }

    private Map<String, Mapper.TypeParser> getMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();

        // builtin mappers
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            mappers.put(type.typeName(), new NumberFieldMapper.TypeParser(type));
        }
        mappers.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
        mappers.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
        mappers.put(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
        mappers.put(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
        mappers.put(TextFieldMapper.CONTENT_TYPE, new TextFieldMapper.TypeParser());
        mappers.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordFieldMapper.TypeParser());
        mappers.put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(FieldAliasMapper.CONTENT_TYPE, new FieldAliasMapper.TypeParser());
        mappers.put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            mappers.put(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, Mapper.TypeParser> entry : mapperPlugin.getMappers().entrySet()) {
                if (mappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Mapper [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(mappers);
    }

    private static final Map<String, MetadataFieldMapper.TypeParser> builtInMetadataMappers = initBuiltInMetadataMappers();

    private static Map<String, MetadataFieldMapper.TypeParser> initBuiltInMetadataMappers() {
        Map<String, MetadataFieldMapper.TypeParser> builtInMetadataMappers;
        // Use a LinkedHashMap for metadataMappers because iteration order matters
        builtInMetadataMappers = new LinkedHashMap<>();
        // _ignored first so that we always load it, even if only _id is requested
        builtInMetadataMappers.put(IgnoredFieldMapper.NAME, new IgnoredFieldMapper.TypeParser());
        // UID second so it will be the first (if no ignored fields) stored field to load
        // (so will benefit from "fields: []" early termination
        builtInMetadataMappers.put(UidFieldMapper.NAME, new UidFieldMapper.TypeParser());
        builtInMetadataMappers.put(IdFieldMapper.NAME, new IdFieldMapper.TypeParser());
        builtInMetadataMappers.put(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser());
        builtInMetadataMappers.put(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser());
        builtInMetadataMappers.put(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser());
        builtInMetadataMappers.put(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser());
        builtInMetadataMappers.put(SeqNoFieldMapper.NAME, new SeqNoFieldMapper.TypeParser());
        //_field_names must be added last so that it has a chance to see all the other mappers
        builtInMetadataMappers.put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser());
        return Collections.unmodifiableMap(builtInMetadataMappers);
    }

    private static Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, MetadataFieldMapper.TypeParser> metadataMappers = new LinkedHashMap<>();
        int i = 0;
        Map.Entry<String, MetadataFieldMapper.TypeParser> fieldNamesEntry = null;
        for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : builtInMetadataMappers.entrySet()) {
            if (i < builtInMetadataMappers.size() - 1) {
                metadataMappers.put(entry.getKey(), entry.getValue());
            } else {
                assert entry.getKey().equals(FieldNamesFieldMapper.NAME) : "_field_names must be the last registered mapper, order counts";
                fieldNamesEntry = entry;
            }
            i++;
        }
        assert fieldNamesEntry != null;

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : mapperPlugin.getMetadataMappers().entrySet()) {
                if (entry.getKey().equals(FieldNamesFieldMapper.NAME)) {
                    throw new IllegalArgumentException("Plugin cannot contain metadata mapper [" + FieldNamesFieldMapper.NAME + "]");
                }
                if (metadataMappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("MetadataFieldMapper [" + entry.getKey() + "] is already registered");
                }
            }
        }

        // we register _field_names here so that it has a chance to see all the other mappers, including from plugins
        metadataMappers.put(fieldNamesEntry.getKey(), fieldNamesEntry.getValue());
        return Collections.unmodifiableMap(metadataMappers);
    }

    private static Function<String, Predicate<String>> getFieldFilter(List<MapperPlugin> mapperPlugins) {
        Function<String, Predicate<String>> fieldFilter = MapperPlugin.NOOP_FIELD_FILTER;
        for (MapperPlugin mapperPlugin : mapperPlugins) {
            fieldFilter = and(fieldFilter, mapperPlugin.getFieldFilter());
        }
        return fieldFilter;
    }

    private static Function<String, Predicate<String>> and(Function<String, Predicate<String>> first,
                                                           Function<String, Predicate<String>> second) {
        //the purpose of this method is to not chain no-op field predicates, so that we can easily find out when no plugins plug in
        //a field filter, hence skip the mappings filtering part as a whole, as it requires parsing mappings into a map.
        if (first == MapperPlugin.NOOP_FIELD_FILTER) {
            return second;
        }
        if (second == MapperPlugin.NOOP_FIELD_FILTER) {
            return first;
        }
        return index -> {
            Predicate<String> firstPredicate = first.apply(index);
            Predicate<String> secondPredicate = second.apply(index);
            if (firstPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
                return secondPredicate;
            }
            if (secondPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
                return firstPredicate;
            }
            return firstPredicate.and(secondPredicate);
        };
    }

    @Override
    protected void configure() {
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(SyncedFlushService.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetaData.class).asEagerSingleton();
        bind(GlobalCheckpointSyncAction.class).asEagerSingleton();
        bind(TransportResyncReplicationAction.class).asEagerSingleton();
        bind(PrimaryReplicaSyncer.class).asEagerSingleton();
    }

    /**
     * A registry for all field mappers.
     */
    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

    public Collection<Function<IndexSettings, Optional<EngineFactory>>> getEngineFactories() {
        return Collections.emptyList();
    }

}
