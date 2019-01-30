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

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Holder class for several ingest related services.
 */
public class IngestService implements ClusterStateApplier {

    public static final String NOOP_PIPELINE_NAME = "_none";

    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final Map<String, Processor.Factory> processorFactories;
    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    private volatile Map<String, Pipeline> pipelines = new HashMap<>();
    private final ThreadPool threadPool;
    private final IngestMetric totalMetrics = new IngestMetric();

    public IngestService(ClusterService clusterService, ThreadPool threadPool,
                         Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry,
                         List<IngestPlugin> ingestPlugins) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.processorFactories = processorFactories(
            ingestPlugins,
            new Processor.Parameters(
                env, scriptService, analysisRegistry,
                threadPool.getThreadContext(), threadPool::relativeTimeInMillis,
                (delay, command) -> threadPool.schedule(
                    TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC, command
                ), this
            )
        );
        this.threadPool = threadPool;
    }

    private static Map<String, Processor.Factory> processorFactories(List<IngestPlugin> ingestPlugins,
        Processor.Parameters parameters) {
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        for (IngestPlugin ingestPlugin : ingestPlugins) {
            Map<String, Processor.Factory> newProcessors = ingestPlugin.getProcessors(parameters);
            for (Map.Entry<String, Processor.Factory> entry : newProcessors.entrySet()) {
                if (processorFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Ingest processor [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(processorFactories);
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerDelete(request, currentState);
            }
        });
    }

    static ClusterState innerDelete(DeletePipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        if (currentIngestMetadata == null) {
            return currentState;
        }
        Map<String, PipelineConfiguration> pipelines = currentIngestMetadata.getPipelines();
        Set<String> toRemove = new HashSet<>();
        for (String pipelineKey : pipelines.keySet()) {
            if (Regex.simpleMatch(request.getId(), pipelineKey)) {
                toRemove.add(pipelineKey);
            }
        }
        if (toRemove.isEmpty() && Regex.isMatchAllPattern(request.getId()) == false) {
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        } else if (toRemove.isEmpty()) {
            return currentState;
        }
        final Map<String, PipelineConfiguration> pipelinesCopy = new HashMap<>(pipelines);
        for (String key : toRemove) {
            pipelinesCopy.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
                .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelinesCopy))
                .build());
        return newState.build();
    }

    /**
     * @return pipeline configuration specified by id. If multiple ids or wildcards are specified multiple pipelines
     * may be returned
     */
    // Returning PipelineConfiguration instead of Pipeline, because Pipeline and Processor interface don't
    // know how to serialize themselves.
    public static List<PipelineConfiguration> getPipelines(ClusterState clusterState, String... ids) {
        IngestMetadata ingestMetadata = clusterState.getMetaData().custom(IngestMetadata.TYPE);
        return innerGetPipelines(ingestMetadata, ids);
    }

    static List<PipelineConfiguration> innerGetPipelines(IngestMetadata ingestMetadata, String... ids) {
        if (ingestMetadata == null) {
            return Collections.emptyList();
        }

        // if we didn't ask for _any_ ID, then we get them all (this is the same as if they ask for '*')
        if (ids.length == 0) {
            return new ArrayList<>(ingestMetadata.getPipelines().values());
        }

        List<PipelineConfiguration> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineConfiguration> entry : ingestMetadata.getPipelines().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(id);
                if (pipeline != null) {
                    result.add(pipeline);
                }
            }
        }
        return result;
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void putPipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request,
        ActionListener<AcknowledgedResponse> listener) throws Exception {
            // validates the pipeline and processor configuration before submitting a cluster update task:
            validatePipeline(ingestInfos, request);
            clusterService.submitStateUpdateTask("put-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return innerPut(request, currentState);
                    }
                });
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline getPipeline(String id) {
        return pipelines.get(id);
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    public IngestInfo info() {
        Map<String, Processor.Factory> processorFactories = getProcessorFactories();
        List<ProcessorInfo> processorInfoList = new ArrayList<>(processorFactories.size());
        for (Map.Entry<String, Processor.Factory> entry : processorFactories.entrySet()) {
            processorInfoList.add(new ProcessorInfo(entry.getKey()));
        }
        return new IngestInfo(processorInfoList);
    }

    Map<String, Pipeline> pipelines() {
        return pipelines;
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        ClusterState state = event.state();
        Map<String, Pipeline> originalPipelines = pipelines;
        innerUpdatePipelines(event.previousState(), state);
        //pipelines changed, so add the old metrics to the new metrics
        if (originalPipelines != pipelines) {
            pipelines.forEach((id, pipeline) -> {
                Pipeline originalPipeline = originalPipelines.get(id);
                if (originalPipeline != null) {
                    pipeline.getMetrics().add(originalPipeline.getMetrics());
                    List<Tuple<Processor, IngestMetric>> oldPerProcessMetrics = new ArrayList<>();
                    List<Tuple<Processor, IngestMetric>> newPerProcessMetrics = new ArrayList<>();
                    getProcessorMetrics(originalPipeline.getCompoundProcessor(), oldPerProcessMetrics);
                    getProcessorMetrics(pipeline.getCompoundProcessor(), newPerProcessMetrics);
                    //Best attempt to populate new processor metrics using a parallel array of the old metrics. This is not ideal since
                    //the per processor metrics may get reset when the arrays don't match. However, to get to an ideal model, unique and
                    //consistent id's per processor and/or semantic equals for each processor will be needed.
                    if (newPerProcessMetrics.size() == oldPerProcessMetrics.size()) {
                        Iterator<Tuple<Processor, IngestMetric>> oldMetricsIterator = oldPerProcessMetrics.iterator();
                        for (Tuple<Processor, IngestMetric> compositeMetric : newPerProcessMetrics) {
                            String type = compositeMetric.v1().getType();
                            IngestMetric metric = compositeMetric.v2();
                            if (oldMetricsIterator.hasNext()) {
                                Tuple<Processor, IngestMetric> oldCompositeMetric = oldMetricsIterator.next();
                                String oldType = oldCompositeMetric.v1().getType();
                                IngestMetric oldMetric = oldCompositeMetric.v2();
                                if (type.equals(oldType)) {
                                    metric.add(oldMetric);
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    /**
     * Recursive method to obtain all of the non-failure processors for given compoundProcessor. Since conditionals are implemented as
     * wrappers to the actual processor, always prefer the actual processor's metric over the conditional processor's metric.
     * @param compoundProcessor The compound processor to start walking the non-failure processors
     * @param processorMetrics The list of {@link Processor} {@link IngestMetric} tuples.
     * @return the processorMetrics for all non-failure processor that belong to the original compoundProcessor
     */
    private static List<Tuple<Processor, IngestMetric>> getProcessorMetrics(CompoundProcessor compoundProcessor,
                                                                    List<Tuple<Processor, IngestMetric>> processorMetrics) {
        //only surface the top level non-failure processors, on-failure processor times will be included in the top level non-failure
        for (Tuple<Processor, IngestMetric> processorWithMetric : compoundProcessor.getProcessorsWithMetrics()) {
            Processor processor = processorWithMetric.v1();
            IngestMetric metric = processorWithMetric.v2();
            if (processor instanceof CompoundProcessor) {
                getProcessorMetrics((CompoundProcessor) processor, processorMetrics);
            } else {
                //Prefer the conditional's metric since it only includes metrics when the conditional evaluated to true.
                if (processor instanceof ConditionalProcessor) {
                    metric = ((ConditionalProcessor) processor).getMetric();
                }
                processorMetrics.add(new Tuple<>(processor, metric));
            }
        }
        return processorMetrics;
    }

    private static Pipeline substitutePipeline(String id, ElasticsearchParseException e) {
        String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
        String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
        String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
        Processor failureProcessor = new AbstractProcessor(tag) {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) {
                throw new IllegalStateException(errorMessage);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        String description = "this is a place holder pipeline, because pipeline with id [" +  id + "] could not be loaded";
        return new Pipeline(id, description, null, new CompoundProcessor(failureProcessor));
    }

    static ClusterState innerPut(PutPipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentIngestMetadata != null) {
            pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }

        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getXContentType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
            .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines))
            .build());
        return newState.build();
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }

        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        Pipeline pipeline = Pipeline.create(request.getId(), pipelineConfig, processorFactories, scriptService);
        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(type) == false && ConditionalProcessor.TYPE.equals(type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(
                        ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message)
                    );
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public void executeBulkRequest(Iterable<DocWriteRequest<?>> actionRequests,
        BiConsumer<IndexRequest, Exception> itemFailureHandler, Consumer<Exception> completionHandler,
        Consumer<IndexRequest> itemDroppedHandler) {

        threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                completionHandler.accept(e);
            }

            @Override
            protected void doRun() {
                for (DocWriteRequest<?> actionRequest : actionRequests) {
                    IndexRequest indexRequest = null;
                    if (actionRequest instanceof IndexRequest) {
                        indexRequest = (IndexRequest) actionRequest;
                    } else if (actionRequest instanceof UpdateRequest) {
                        UpdateRequest updateRequest = (UpdateRequest) actionRequest;
                        indexRequest = updateRequest.docAsUpsert() ? updateRequest.doc() : updateRequest.upsertRequest();
                    }
                    if (indexRequest == null) {
                        continue;
                    }
                    String pipelineId = indexRequest.getPipeline();
                    if (NOOP_PIPELINE_NAME.equals(pipelineId) == false) {
                        try {
                            Pipeline pipeline = pipelines.get(pipelineId);
                            if (pipeline == null) {
                                throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
                            }
                            innerExecute(indexRequest, pipeline, itemDroppedHandler);
                            //this shouldn't be needed here but we do it for consistency with index api
                            // which requires it to prevent double execution
                            indexRequest.setPipeline(NOOP_PIPELINE_NAME);
                        } catch (Exception e) {
                            itemFailureHandler.accept(indexRequest, e);
                        }
                    }
                }
                completionHandler.accept(null);
            }
        });
    }

    public IngestStats stats() {
        IngestStats.Builder statsBuilder = new IngestStats.Builder();
        statsBuilder.addTotalMetrics(totalMetrics);
        pipelines.forEach((id, pipeline) -> {
            CompoundProcessor rootProcessor = pipeline.getCompoundProcessor();
            statsBuilder.addPipelineMetrics(id, pipeline.getMetrics());
            List<Tuple<Processor, IngestMetric>> processorMetrics = new ArrayList<>();
            getProcessorMetrics(rootProcessor, processorMetrics);
            processorMetrics.forEach(t -> {
                Processor processor = t.v1();
                IngestMetric processorMetric = t.v2();
                statsBuilder.addProcessorMetrics(id, getProcessorName(processor), processorMetric);
            });
        });
        return statsBuilder.build();
    }

    //package private for testing
    static String getProcessorName(Processor processor){
        // conditionals are implemented as wrappers around the real processor, so get the real processor for the correct type for the name
        if(processor instanceof ConditionalProcessor){
            processor = ((ConditionalProcessor) processor).getProcessor();
        }
        StringBuilder sb = new StringBuilder(5);
        sb.append(processor.getType());

        if(processor instanceof PipelineProcessor){
            String pipelineName = ((PipelineProcessor) processor).getPipelineName();
            sb.append(":");
            sb.append(pipelineName);
        }
        String tag = processor.getTag();
        if(tag != null && !tag.isEmpty()){
            sb.append(":");
            sb.append(tag);
        }
        return sb.toString();
    }

    private void innerExecute(IndexRequest indexRequest, Pipeline pipeline, Consumer<IndexRequest> itemDroppedHandler) throws Exception {
        if (pipeline.getProcessors().isEmpty()) {
            return;
        }

        long startTimeInNanos = System.nanoTime();
        // the pipeline specific stat holder may not exist and that is fine:
        // (e.g. the pipeline may have been removed while we're ingesting a document
        try {
            totalMetrics.preIngest();
            String index = indexRequest.index();
            String type = indexRequest.type();
            String id = indexRequest.id();
            String routing = indexRequest.routing();
            Long version = indexRequest.version();
            VersionType versionType = indexRequest.versionType();
            Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
            IngestDocument ingestDocument = new IngestDocument(index, type, id, routing, null, version, versionType, sourceAsMap);
            if (pipeline.execute(ingestDocument) == null) {
                itemDroppedHandler.accept(indexRequest);
            } else {
                Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
                //it's fine to set all metadata fields all the time, as ingest document holds their starting values
                //before ingestion, which might also get modified during ingestion.
                indexRequest.index((String) metadataMap.get(IngestDocument.MetaData.INDEX));
                indexRequest.type((String) metadataMap.get(IngestDocument.MetaData.TYPE));
                indexRequest.id((String) metadataMap.get(IngestDocument.MetaData.ID));
                indexRequest.routing((String) metadataMap.get(IngestDocument.MetaData.ROUTING));
                indexRequest.version(((Number) metadataMap.get(IngestDocument.MetaData.VERSION)).longValue());
                if (metadataMap.get(IngestDocument.MetaData.VERSION_TYPE) != null) {
                    indexRequest.versionType(VersionType.fromString((String) metadataMap.get(IngestDocument.MetaData.VERSION_TYPE)));
                }
                indexRequest.source(ingestDocument.getSourceAndMetadata());
            }
        } catch (Exception e) {
            totalMetrics.ingestFailed();
            throw e;
        } finally {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            totalMetrics.postIngest(ingestTimeInMillis);
        }
    }

    private void innerUpdatePipelines(ClusterState previousState, ClusterState state) {
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        IngestMetadata previousIngestMetadata = previousState.getMetaData().custom(IngestMetadata.TYPE);
        if (Objects.equals(ingestMetadata, previousIngestMetadata)) {
            return;
        }

        Map<String, Pipeline> pipelines = new HashMap<>();
        List<ElasticsearchParseException> exceptions = new ArrayList<>();
        for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
            try {
                pipelines.put(
                    pipeline.getId(),
                    Pipeline.create(pipeline.getId(), pipeline.getConfigAsMap(), processorFactories, scriptService)
                );
            } catch (ElasticsearchParseException e) {
                pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), e));
                exceptions.add(e);
            } catch (Exception e) {
                ElasticsearchParseException parseException = new ElasticsearchParseException(
                    "Error updating pipeline with id [" + pipeline.getId() + "]", e);
                pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), parseException));
                exceptions.add(parseException);
            }
        }
        this.pipelines = Collections.unmodifiableMap(pipelines);
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

}
