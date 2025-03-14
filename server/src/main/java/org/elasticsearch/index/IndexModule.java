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

package org.elasticsearch.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.search.QueryCache;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.metadata.NodeContext;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataTypes;

/**
 * IndexModule represents the central extension point for index level custom implementations like:
 * <ul>
 *      <li>{@link IndexStorePlugin.DirectoryFactory} - Custom {@link IndexStorePlugin.DirectoryFactory} instances can be registered
 *      via {@link IndexStorePlugin}</li>
 *      <li>{@link IndexEventListener} - Custom {@link IndexEventListener} instances can be registered via
 *      {@link #addIndexEventListener(IndexEventListener)}</li>
 *      <li>Settings update listener - Custom settings update listener can be registered via
 *      {@link #addSettingsUpdateConsumer(Setting, Consumer)}</li>
 * </ul>
 */
public final class IndexModule {

    public static final Setting<Boolean> NODE_STORE_ALLOW_MMAP = Setting.boolSetting(
        "node.store.allow_mmap",
        true,
        Property.NodeScope
    );

    private static final FsDirectoryFactory DEFAULT_DIRECTORY_FACTORY = new FsDirectoryFactory();

    public static final Setting<String> INDEX_STORE_TYPE_SETTING =
            new Setting<>("index.store.type", "fs", Function.identity(), DataTypes.STRING, Property.IndexScope, Property.NodeScope, Property.Final);

    // whether to use the query cache
    public static final Setting<Boolean> INDEX_QUERY_CACHE_ENABLED_SETTING =
            Setting.boolSetting("index.queries.cache.enabled", true, Property.IndexScope);

    // for test purposes only
    public static final Setting<Boolean> INDEX_QUERY_CACHE_EVERYTHING_SETTING =
        Setting.boolSetting("index.queries.cache.everything", false, Property.IndexScope);

    private final IndexSettings indexSettings;
    private final AnalysisRegistry analysisRegistry;
    private final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders;
    private final Set<IndexEventListener> indexEventListeners = new HashSet<>();
    private final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories;
    private final List<IndexingOperationListener> indexOperationListeners = new ArrayList<>();
    private final AtomicBoolean frozen = new AtomicBoolean(false);

    /**
     * Construct the index module for the index with the specified index settings. The index module contains extension points for plugins
     * via {@link org.elasticsearch.plugins.PluginsService#onIndexModule(IndexModule)}.
     *
     * @param indexSettings             the index settings
     * @param analysisRegistry          the analysis registry
     * @param engineFactoryProviders    list of engine factory providers
     * @param directoryFactories        the available store types
     */
    public IndexModule(
            final IndexSettings indexSettings,
            final AnalysisRegistry analysisRegistry,
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders,
            final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories) {
        this.indexSettings = indexSettings;
        this.analysisRegistry = analysisRegistry;
        this.engineFactoryProviders = engineFactoryProviders;
        this.directoryFactories = Collections.unmodifiableMap(directoryFactories);
    }

    /**
     * Adds a Setting and it's consumer for this index.
     */
    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer);
    }

    /**
     * Adds a Setting, it's consumer and validator for this index.
     */
    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer, validator);
    }

    /**
     * Returns the index {@link Settings} for this index
     */
    public Settings getSettings() {
        return indexSettings.getSettings();
    }

    /**
     * Returns the index this module is associated with
     */
    public Index getIndex() {
        return indexSettings.getIndex();
    }

    /**
     * Adds an {@link IndexEventListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addIndexEventListener(IndexEventListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexEventListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexEventListeners.add(listener);
    }

    /**
     * Adds an {@link IndexingOperationListener} for this index. All listeners added here
     * are maintained for the entire index lifecycle on this node. Once an index is closed or deleted these
     * listeners go out of scope.
     * <p>
     * Note: an index might be created on a node multiple times. For instance if the last shard from an index is
     * relocated to another node the internal representation will be destroyed which includes the registered listeners.
     * Once the node holds at least one shard of an index all modules are reloaded and listeners are registered again.
     * Listeners can't be unregistered they will stay alive for the entire time the index is allocated on a node.
     * </p>
     */
    public void addIndexOperationListener(IndexingOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }

        this.indexOperationListeners.add(listener);
    }

    IndexEventListener freeze() { // pkg private for testing
        if (this.frozen.compareAndSet(false, true)) {
            return new CompositeIndexEventListener(indexSettings, indexEventListeners);
        } else {
            throw new IllegalStateException("already frozen");
        }
    }

    public static boolean isBuiltinType(String storeType) {
        for (Type type : Type.values()) {
            if (type.match(storeType)) {
                return true;
            }
        }
        return false;
    }


    public enum Type {
        HYBRIDFS("hybridfs"),
        NIOFS("niofs"),
        MMAPFS("mmapfs"),
        FS("fs");

        private final String settingsKey;

        Type(final String settingsKey) {
            this.settingsKey = settingsKey;
        }

        private static final Map<String, Type> TYPES;

        static {
            final Map<String, Type> types = new HashMap<>(4);
            for (final Type type : values()) {
                types.put(type.settingsKey, type);
            }
            TYPES = Collections.unmodifiableMap(types);
        }

        public String getSettingsKey() {
            return this.settingsKey;
        }

        public static Type fromSettingsKey(final String key) {
            final Type type = TYPES.get(key);
            if (type == null) {
                throw new IllegalArgumentException("no matching store type for [" + key + "]");
            }
            return type;
        }

        /**
         * Returns true iff this settings matches the type.
         */
        public boolean match(String setting) {
            return getSettingsKey().equals(setting);
        }

    }

    public static Type defaultStoreType(final boolean allowMmap) {
        if (allowMmap && Constants.JRE_IS_64BIT) {
            return Type.HYBRIDFS;
        } else {
            return Type.NIOFS;
        }
    }

    public IndexService newIndexService(
            NodeContext nodeContext,
            IndexService.IndexCreationContext indexCreationContext,
            NodeEnvironment environment,
            IndexService.ShardStoreDeleter shardStoreDeleter,
            CircuitBreakerService circuitBreakerService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            QueryCache indicesQueryCache,
            Supplier<TableInfo> getTable) throws IOException {

        final IndexEventListener eventListener = freeze();
        eventListener.beforeIndexCreated(indexSettings.getIndex(), indexSettings.getSettings());
        final IndexStorePlugin.DirectoryFactory directoryFactory = getDirectoryFactory(indexSettings, directoryFactories);
        final QueryCache queryCache;
        if (indexSettings.getValue(INDEX_QUERY_CACHE_ENABLED_SETTING)) {
            queryCache = indicesQueryCache;
        } else {
            queryCache = DisabledQueryCache.instance();
        }
        return new IndexService(
            nodeContext,
            indexSettings,
            indexCreationContext,
            environment,
            shardStoreDeleter,
            analysisRegistry,
            engineFactoryProviders,
            circuitBreakerService,
            bigArrays,
            threadPool,
            queryCache,
            directoryFactory,
            eventListener,
            getTable,
            indexOperationListeners
        );
    }

    private static IndexStorePlugin.DirectoryFactory getDirectoryFactory(
            final IndexSettings indexSettings, final Map<String, IndexStorePlugin.DirectoryFactory> indexStoreFactories) {
        final String storeType = indexSettings.getValue(INDEX_STORE_TYPE_SETTING);
        final Type type;
        final Boolean allowMmap = NODE_STORE_ALLOW_MMAP.getWithFallback(indexSettings.getNodeSettings());
        if (storeType.isEmpty() || Type.FS.getSettingsKey().equals(storeType)) {
            type = defaultStoreType(allowMmap);
        } else {
            if (isBuiltinType(storeType)) {
                type = Type.fromSettingsKey(storeType);
            } else {
                type = null;
            }
        }
        if (allowMmap == false && (type == Type.MMAPFS || type == Type.HYBRIDFS)) {
            throw new IllegalArgumentException("store type [" + storeType + "] is not allowed because mmap is disabled");
        }
        final IndexStorePlugin.DirectoryFactory factory;
        if (storeType.isEmpty() || isBuiltinType(storeType)) {
            factory = DEFAULT_DIRECTORY_FACTORY;
        } else {
            factory = indexStoreFactories.get(storeType);
            if (factory == null) {
                throw new IllegalArgumentException("Unknown store type [" + storeType + "]");
            }
        }
        return factory;
    }

    private void ensureNotFrozen() {
        if (this.frozen.get()) {
            throw new IllegalStateException("Can't modify IndexModule once the index service has been created");
        }
    }

}
