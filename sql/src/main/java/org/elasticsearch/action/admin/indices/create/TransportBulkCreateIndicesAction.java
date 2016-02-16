/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.action.admin.indices.create;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.crate.jobs.JobContextService;
import io.crate.jobs.KillAllListener;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 * creates one or more indices within one cluster-state-update-task
 *
 * This is more or less a more optimized version of {@link MetaDataCreateIndexService}
 *
 * It also has some limitations:
 *
 *  - all indices must actually have the same name pattern (only the first index is used to figure out which templates to use)
 *  - and alias / mappings / etc. are not taken from the request
 */
@Singleton
public class TransportBulkCreateIndicesAction
        extends TransportMasterNodeAction<BulkCreateIndicesRequest, BulkCreateIndicesResponse>
        implements KillAllListener {

    public static final String NAME = "indices:admin/bulk_create";

    private static final DefaultIndexTemplateFilter DEFAULT_INDEX_TEMPLATE_FILTER = new DefaultIndexTemplateFilter();

    private final IndexTemplateFilter indexTemplateFilter;
    private final AliasValidator aliasValidator;
    private final Version version;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final MetaDataCreateIndexService createIndexService;
    private final Environment environment;
    private final MetaDataService metaDataService;

    private final Object pendingLock = new Object();
    private final Queue<PendingOperation> pendingOperations = new ArrayDeque<>();
    private volatile int activeOperations = 0;
    private volatile long lastKillAllEvent = System.nanoTime();

    @Inject
    protected TransportBulkCreateIndicesAction(Settings settings,
                                               JobContextService jobContextService,
                                               TransportService transportService,
                                               Environment environment,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               MetaDataService metaDataService,
                                               AliasValidator aliasValidator,
                                               Version version,
                                               IndicesService indicesService,
                                               AllocationService allocationService,
                                               MetaDataCreateIndexService createIndexService,
                                               Set<IndexTemplateFilter> indexTemplateFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, BulkCreateIndicesRequest.class);
        this.environment = environment;
        this.metaDataService = metaDataService;
        this.aliasValidator = aliasValidator;
        this.version = version;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;

        jobContextService.addListener(this);

        if (indexTemplateFilters.isEmpty()) {
            this.indexTemplateFilter = DEFAULT_INDEX_TEMPLATE_FILTER;
        } else {
            IndexTemplateFilter[] templateFilters = new IndexTemplateFilter[indexTemplateFilters.size() + 1];
            templateFilters[0] = DEFAULT_INDEX_TEMPLATE_FILTER;
            int i = 1;
            for (IndexTemplateFilter indexTemplateFilter : indexTemplateFilters) {
                templateFilters[i++] = indexTemplateFilter;
            }
            this.indexTemplateFilter = new IndexTemplateFilter.Compound(templateFilters);
        }
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected BulkCreateIndicesResponse newResponse() {
        return new BulkCreateIndicesResponse();
    }

    private void triggerNext() {
        PendingOperation pendingOperation = null;
        synchronized (pendingLock) {
            activeOperations--;
            if (activeOperations == 0) {
                pendingOperation = pendingOperations.poll();
            }
        }

        if (pendingOperation == null) {
            return;
        }
        masterOperation(pendingOperation.request, clusterService.state(), pendingOperation.responseListener);
    }

    @Override
    protected void masterOperation(final BulkCreateIndicesRequest request,
                                   final ClusterState state,
                                   final ActionListener<BulkCreateIndicesResponse> responseListener) throws ElasticsearchException {

        if (request.indices().isEmpty()) {
            responseListener.onResponse(new BulkCreateIndicesResponse(true));
            return;
        }

        synchronized (pendingLock) {
            if (activeOperations > 0) {
                pendingOperations.add(new PendingOperation(request, responseListener));
                return;
            }
            activeOperations++;
        }
        final long timeStart = System.nanoTime();

        final ActionListener<BulkCreateIndicesResponse> listener = new PendingTriggeringActionListener(responseListener);
        final Map<Semaphore, Collection<String>> locked = new HashMap<>();
        final Map<Semaphore, Collection<String>> unlocked = metaDataService.indexMetaDataLocks(request.indices());

        try {
            tryAcquireLocksAndRemoveExistingIndices(state, locked, unlocked, 0L);
        } catch (InterruptedException e) {
            unlockIndices(locked);
            listener.onFailure(e);
            return;
        }
        final ActionListener<ClusterStateUpdateResponse> stateUpdateListener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                locked.clear();  // locks are already unlocked;

                if (!unlocked.isEmpty()) {
                    tryAcquireLocksAndCreateIndicesThreaded(
                            timeStart, request, clusterService.state(), locked, unlocked, this);
                } else {
                    listener.onResponse(new BulkCreateIndicesResponse(true));
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        };

        if (locked.isEmpty() && unlocked.isEmpty()) {
            // all indices already existed?
            listener.onResponse(new BulkCreateIndicesResponse(true));
            return;
        }

        if (timeStart <= lastKillAllEvent) {
            unlockIndices(locked);
            responseListener.onFailure(new CancellationException());
            return;
        }
        if (!locked.isEmpty()) {
            createIndices(request, locked, stateUpdateListener);
        } else {
            tryAcquireLocksAndCreateIndicesThreaded(timeStart, request, state, locked, unlocked, stateUpdateListener);
        }
    }
    private void tryAcquireLocksAndCreateIndicesThreaded(final long timeStart,
                                                         final BulkCreateIndicesRequest request,
                                                         final ClusterState state,
                                                         final Map<Semaphore, Collection<String>> locked,
                                                         final Map<Semaphore, Collection<String>> unlocked,
                                                         final ActionListener<ClusterStateUpdateResponse> stateUpdateListener) {
        try {
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (timeStart < lastKillAllEvent) {
                            stateUpdateListener.onFailure(new CancellationException());
                            return;
                        }
                        long elapsed = System.nanoTime() - timeStart;
                        long timeout = request.timeout().nanos();
                        if (elapsed >= timeout) {
                            stateUpdateListener.onFailure(new ProcessClusterEventTimeoutException(request.timeout(), "acquire index lock"));
                            return;
                        }
                        tryAcquireLocksAndRemoveExistingIndices(state, locked, unlocked, 0);
                        if (locked.isEmpty() && !unlocked.isEmpty()) {
                            long remainingTimeout = timeout - elapsed;
                            tryAcquireLocksAndRemoveExistingIndices(state, locked, unlocked, remainingTimeout);
                        } else {
                            createIndices(request, locked, stateUpdateListener);
                        }
                    } catch (InterruptedException e) {
                        stateUpdateListener.onFailure(
                                new ProcessClusterEventTimeoutException(request.timeout(), "acquire index lock"));
                    } finally {
                        unlockIndices(locked);
                    }
                }
            });
        } catch (EsRejectedExecutionException | RejectedExecutionException e) {
            unlockIndices(locked);
            stateUpdateListener.onFailure(e);
        }
    }

    private void tryAcquireLocksAndRemoveExistingIndices(ClusterState state,
                                                         Map<Semaphore, Collection<String>> locked,
                                                         Map<Semaphore, Collection<String>> unlocked,
                                                         long timeout) throws InterruptedException {
        Iterator<Map.Entry<Semaphore, Collection<String>>> unlockedIterator = unlocked.entrySet().iterator();
        while (unlockedIterator.hasNext()) {
            Map.Entry<Semaphore, Collection<String>> entry = unlockedIterator.next();

            Set<String> validIndices = Sets.newHashSet(entry.getValue());
            Iterator<String> iterator = validIndices.iterator();
            while (iterator.hasNext()) {
                String index = iterator.next();

                if (state.routingTable().hasIndex(index) || state.metaData().hasIndex(index)) {
                    iterator.remove();
                }
            }

            if (validIndices.isEmpty()) {
                unlockedIterator.remove();
                continue;
            }

            Semaphore lock = entry.getKey();
            if (lockAcquired(timeout, lock)) {
                locked.put(lock, validIndices);
                unlockedIterator.remove();
            } else{
                entry.setValue(validIndices);
            }
        }
    }

    private void unlockIndices(Map<Semaphore, Collection<String>> lockedIndices) {
        for (Semaphore semaphore : lockedIndices.keySet()) {
            semaphore.release();
        }
    }

    private boolean lockAcquired(long timeout, Semaphore lock) throws InterruptedException {
        boolean b = lock.tryAcquire(timeout, TimeUnit.NANOSECONDS);
        if (!b && timeout > 0) {
            throw new InterruptedException();
        }
        return b;
    }

    protected ClusterState executeCreateIndices(ClusterState currentState,
                                                BulkCreateIndicesRequest request) throws Exception {
        /**
         * This code is more or less the same as the stuff in {@link MetaDataCreateIndexService}
         * but optimized for bulk operation without separate mapping/alias/index settings.
         */

        List<String> indicesToCreate = new ArrayList<>(request.indices().size());
        String removalReason = null;
        String testIndex = null;
        try {
            validateAndFilterExistingIndices(currentState, indicesToCreate, request);
            if (indicesToCreate.isEmpty()) {
                return currentState;
            }

            Map<String, IndexMetaData.Custom> customs = Maps.newHashMap();
            Map<String, Map<String, Object>> mappings = Maps.newHashMap();
            Map<String, AliasMetaData> templatesAliases = Maps.newHashMap();
            List<String> templateNames = Lists.newArrayList();

            List<IndexTemplateMetaData> templates = findTemplates(request, currentState, indexTemplateFilter);
            applyTemplates(customs, mappings, templatesAliases, templateNames, templates);
            File mappingsDir = new File(environment.configFile().toFile(), "mappings");
            if (mappingsDir.isDirectory()) {
                addMappingFromMappingsFile(mappings, mappingsDir, request);
            }

            Settings indexSettings = createIndexSettings(currentState, templates);

            testIndex = indicesToCreate.get(0);
            indicesService.createIndex(testIndex, indexSettings, clusterService.localNode().id());

            // now add the mappings
            IndexService indexService = indicesService.indexServiceSafe(testIndex);
            MapperService mapperService = indexService.mapperService();
            // first, add the default mapping
            if (mappings.containsKey(MapperService.DEFAULT_MAPPING)) {
                try {
                    mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedXContent(XContentFactory.jsonBuilder().map(mappings.get(MapperService.DEFAULT_MAPPING)).string()), false, false);
                } catch (Exception e) {
                    removalReason = "failed on parsing default mapping on index creation";
                    throw new MapperParsingException("mapping [" + MapperService.DEFAULT_MAPPING + "]", e);
                }
            }
            for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
                if (entry.getKey().equals(MapperService.DEFAULT_MAPPING)) {
                    continue;
                }
                try {
                    // apply the default here, its the first time we parse it
                    mapperService.merge(entry.getKey(), new CompressedXContent(XContentFactory.jsonBuilder().map(entry.getValue()).string()), true, false);
                } catch (Exception e) {
                    removalReason = "failed on parsing mappings on index creation";
                    throw new MapperParsingException("mapping [" + entry.getKey() + "]", e);
                }
            }

            IndexQueryParserService indexQueryParserService = indexService.queryParserService();
            for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                if (aliasMetaData.filter() != null) {
                    aliasValidator.validateAliasFilter(aliasMetaData.alias(), aliasMetaData.filter().uncompressed(), indexQueryParserService);
                }
            }

            // now, update the mappings with the actual source
            Map<String, MappingMetaData> mappingsMetaData = Maps.newHashMap();
            for (DocumentMapper mapper : mapperService.docMappers(true)) {
                MappingMetaData mappingMd = new MappingMetaData(mapper);
                mappingsMetaData.put(mapper.type(), mappingMd);
            }

            MetaData.Builder newMetaDataBuilder = MetaData.builder(currentState.metaData());
            for (String index : indicesToCreate) {
                final IndexMetaData.Builder indexMetaDataBuilder =
                        IndexMetaData.builder(index).settings(indexSettings);

                for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                    indexMetaDataBuilder.putMapping(mappingMd);
                }
                for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                    indexMetaDataBuilder.putAlias(aliasMetaData);
                }
                for (Map.Entry<String, IndexMetaData.Custom> customEntry : customs.entrySet()) {
                    indexMetaDataBuilder.putCustom(customEntry.getKey(), customEntry.getValue());
                }
                indexMetaDataBuilder.state(IndexMetaData.State.OPEN);

                final IndexMetaData indexMetaData;
                try {
                    indexMetaData = indexMetaDataBuilder.build();
                } catch (Exception e) {
                    removalReason = "failed to build index metadata";
                    throw e;
                }
                logger.info("[{}] creating index, cause [bulk], templates {}, shards [{}]/[{}], mappings {}",
                        index, templateNames, indexMetaData.getNumberOfShards(), indexMetaData.getNumberOfReplicas(), mappings.keySet());

                indexService.indicesLifecycle().beforeIndexAddedToCluster(new Index(index), indexMetaData.getSettings());
                newMetaDataBuilder.put(indexMetaData, false);
            }
            MetaData newMetaData = newMetaDataBuilder.build();

            ClusterState updatedState = ClusterState.builder(currentState).metaData(newMetaData).build();
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
            for (String index : indicesToCreate) {
                routingTableBuilder.addAsNew(updatedState.metaData().index(index));
            }
            RoutingAllocation.Result routingResult = allocationService.reroute(
                    ClusterState.builder(updatedState).routingTable(routingTableBuilder).build());
            updatedState = ClusterState.builder(updatedState).routingResult(routingResult).build();

            removalReason = "cleaning up after validating index on master";
            return updatedState;
        } finally {
            if (testIndex != null) {
                // index was partially created - need to clean up
                indicesService.deleteIndex(testIndex, removalReason != null ? removalReason : "failed to create index");
            }
        }
    }

    private void createIndices(final BulkCreateIndicesRequest request,
                               final Map<Semaphore, Collection<String>> lockedIndices,
                               ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("create-indices [" +  "], cause [" +  "]", Priority.URGENT,
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                unlockIndices(lockedIndices);
                super.onAllNodesAcked(t);
            }

            @Override
            public void onAckTimeout() {
                unlockIndices(lockedIndices);
                super.onAckTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                unlockIndices(lockedIndices);
                super.onFailure(source, t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return executeCreateIndices(currentState, request);
            }
        });

    }

    private void addMappingFromMappingsFile(Map<String, Map<String, Object>> mappings, File mappingsDir, BulkCreateIndicesRequest request) {
        for (String index : request.indices()) {
            // first index level
            File indexMappingsDir = new File(mappingsDir, index);
            if (indexMappingsDir.isDirectory()) {
                addMappings(mappings, indexMappingsDir);
            }

            // second is the _default mapping
            File defaultMappingsDir = new File(mappingsDir, "_default");
            if (defaultMappingsDir.isDirectory()) {
                addMappings(mappings, defaultMappingsDir);
            }
        }
    }

    private void validateAndFilterExistingIndices(ClusterState currentState,
                                                  List<String> indicesToCreate,
                                                  BulkCreateIndicesRequest request) {
        for (String index : request.indices()) {
            try {
                createIndexService.validateIndexName(index, currentState);
                indicesToCreate.add(index);
            } catch (IndexAlreadyExistsException e) {
                // ignore
            }
        }
    }

    private Settings createIndexSettings(ClusterState currentState, List<IndexTemplateMetaData> templates) {
        Settings.Builder indexSettingsBuilder = settingsBuilder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            indexSettingsBuilder.put(templates.get(i).settings());
        }
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                    settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5));
        }
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_NUMBER_OF_REPLICAS) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS,
                    settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1));
        }
        if (settings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS) != null
                && indexSettingsBuilder.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS,
                    settings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS));
        }

        if (indexSettingsBuilder.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
            DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.smallest(version, nodes.smallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetaData.SETTING_VERSION_CREATED, createdVersion);
        }

        if (indexSettingsBuilder.get(IndexMetaData.SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_CREATION_DATE, System.currentTimeMillis());
        }

        indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, Strings.randomBase64UUID());

        return indexSettingsBuilder.build();
    }

    private void addMappings(Map<String, Map<String, Object>> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        for (File mappingFile : mappingsFiles) {
            if (mappingFile.isHidden()) {
                continue;
            }
            int lastDotIndex = mappingFile.getName().lastIndexOf('.');
            String mappingType = lastDotIndex != -1 ? mappingFile.getName().substring(0, lastDotIndex) : mappingFile.getName();
            try {
                String mappingSource = Streams.copyToString(new InputStreamReader(new FileInputStream(mappingFile), Charsets.UTF_8));
                if (mappings.containsKey(mappingType)) {
                    XContentHelper.mergeDefaults(mappings.get(mappingType), parseMapping(mappingSource));
                } else {
                    mappings.put(mappingType, parseMapping(mappingSource));
                }
            } catch (Exception e) {
                logger.warn("failed to read / parse mapping [" + mappingType + "] from location [" + mappingFile + "], ignoring...", e);
            }
        }
    }

    private void applyTemplates(Map<String, IndexMetaData.Custom> customs,
                                Map<String, Map<String, Object>> mappings,
                                Map<String, AliasMetaData> templatesAliases,
                                List<String> templateNames,
                                List<IndexTemplateMetaData> templates) throws Exception {

        for (IndexTemplateMetaData template : templates) {
            templateNames.add(template.getName());
            for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                if (mappings.containsKey(cursor.key)) {
                    XContentHelper.mergeDefaults(mappings.get(cursor.key), parseMapping(cursor.value.string()));
                } else {
                    mappings.put(cursor.key, parseMapping(cursor.value.string()));
                }
            }
            // handle custom
            for (ObjectObjectCursor<String, IndexMetaData.Custom> cursor : template.customs()) {
                String type = cursor.key;
                IndexMetaData.Custom custom = cursor.value;
                IndexMetaData.Custom existing = customs.get(type);
                if (existing == null) {
                    customs.put(type, custom);
                } else {
                    IndexMetaData.Custom merged = existing.mergeWith(custom);
                    customs.put(type, merged);
                }
            }
            //handle aliases
            for (ObjectObjectCursor<String, AliasMetaData> cursor : template.aliases()) {
                AliasMetaData aliasMetaData = cursor.value;
                templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
            }
        }
    }

    private List<IndexTemplateMetaData> findTemplates(BulkCreateIndicesRequest request,
                                                      ClusterState state,
                                                      IndexTemplateFilter indexTemplateFilter) {
        List<IndexTemplateMetaData> templates = new ArrayList<>();
        CreateIndexClusterStateUpdateRequest dummyRequest =
                new CreateIndexClusterStateUpdateRequest(request, "bulk-create", request.indices().iterator().next(), false);

        // note: only use the first index name to see if template matches.
        // this means
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData template = cursor.value;

            if (indexTemplateFilter.apply(dummyRequest, template)) {
                templates.add(template);
            }
        }
        CollectionUtil.timSort(templates, new Comparator<IndexTemplateMetaData>() {
            @Override
            public int compare(IndexTemplateMetaData o1, IndexTemplateMetaData o2) {
                return o2.order() - o1.order();
            }
        });
        return templates;
    }

    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        try (XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping", e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(BulkCreateIndicesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, Iterables.toArray(request.indices(), String.class));
    }

    @Override
    public void killAllJobs(long timestamp) {
        lastKillAllEvent = timestamp;
        synchronized (pendingLock) {
            PendingOperation pendingOperation;
            while ( (pendingOperation = pendingOperations.poll()) != null) {
                pendingOperation.responseListener.onFailure(new CancellationException());
            }
        }
    }

    @Override
    public void killJob(UUID jobId) {
        synchronized (pendingLock) {
            Iterator<PendingOperation> it = pendingOperations.iterator();
            while (it.hasNext()) {
                PendingOperation pendingOperation = it.next();
                if (pendingOperation.request.jobId().equals(jobId)) {
                    pendingOperation.responseListener.onFailure(new CancellationException());
                    it.remove();
                }
            }
        }
    }

    private static class DefaultIndexTemplateFilter implements IndexTemplateFilter {
        @Override
        public boolean apply(CreateIndexClusterStateUpdateRequest request, IndexTemplateMetaData template) {
            return Regex.simpleMatch(template.template(), request.index());
        }
    }

    static class PendingOperation {

        private final BulkCreateIndicesRequest request;
        private final ActionListener<BulkCreateIndicesResponse> responseListener;

        public PendingOperation(BulkCreateIndicesRequest request, ActionListener<BulkCreateIndicesResponse> responseListener) {
            this.request = request;
            this.responseListener = responseListener;
        }
    }

    private class PendingTriggeringActionListener implements ActionListener<BulkCreateIndicesResponse> {
        private final ActionListener<BulkCreateIndicesResponse> responseListener;

        public PendingTriggeringActionListener(ActionListener<BulkCreateIndicesResponse> responseListener) {
            this.responseListener = responseListener;
        }

        @Override
        public void onResponse(BulkCreateIndicesResponse bulkCreateIndicesRequest) {
            triggerNext();
            responseListener.onResponse(bulkCreateIndicesRequest);
        }

        @Override
        public void onFailure(Throwable e) {
            triggerNext();
            responseListener.onFailure(e);
        }
    }
}
