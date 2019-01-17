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
import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.elasticsearch.script.ScriptService;

/**
 * A pipeline is a list of {@link Processor} instances grouped under a unique id.
 */
public final class Pipeline {

    public static final String DESCRIPTION_KEY = "description";
    public static final String PROCESSORS_KEY = "processors";
    public static final String VERSION_KEY = "version";
    public static final String ON_FAILURE_KEY = "on_failure";

    private final String id;
    @Nullable
    private final String description;
    @Nullable
    private final Integer version;
    private final CompoundProcessor compoundProcessor;
    private final IngestMetric metrics;
    private final LongSupplier relativeTimeProvider;

    public Pipeline(String id, @Nullable String description, @Nullable Integer version, CompoundProcessor compoundProcessor) {
        this(id, description, version, compoundProcessor, System::nanoTime);
    }

    //package private for testing
    Pipeline(String id, @Nullable String description, @Nullable Integer version, CompoundProcessor compoundProcessor,
             LongSupplier relativeTimeProvider) {
        this.id = id;
        this.description = description;
        this.compoundProcessor = compoundProcessor;
        this.version = version;
        this.metrics = new IngestMetric();
        this.relativeTimeProvider = relativeTimeProvider;
    }

    public static Pipeline create(String id, Map<String, Object> config,
        Map<String, Processor.Factory> processorFactories, ScriptService scriptService) throws Exception {
        String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
        Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
        List<Map<String, Object>> processorConfigs = ConfigurationUtils.readList(null, null, config, PROCESSORS_KEY);
        List<Processor> processors = ConfigurationUtils.readProcessorConfigs(processorConfigs, scriptService, processorFactories);
        List<Map<String, Object>> onFailureProcessorConfigs =
                ConfigurationUtils.readOptionalList(null, null, config, ON_FAILURE_KEY);
        List<Processor> onFailureProcessors =
            ConfigurationUtils.readProcessorConfigs(onFailureProcessorConfigs, scriptService, processorFactories);
        if (config.isEmpty() == false) {
            throw new ElasticsearchParseException("pipeline [" + id +
                    "] doesn't support one or more provided configuration parameters " + Arrays.toString(config.keySet().toArray()));
        }
        if (onFailureProcessorConfigs != null && onFailureProcessors.isEmpty()) {
            throw new ElasticsearchParseException("pipeline [" + id + "] cannot have an empty on_failure option defined");
        }
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.unmodifiableList(processors),
                Collections.unmodifiableList(onFailureProcessors));
        return new Pipeline(id, description, version, compoundProcessor);
    }

    /**
     * Modifies the data of a document to be indexed based on the processor this pipeline holds
     */
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        long startTimeInNanos = relativeTimeProvider.getAsLong();
        try {
            metrics.preIngest();
            return compoundProcessor.execute(ingestDocument);
        } catch (Exception e) {
            metrics.ingestFailed();
            throw e;
        } finally {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTimeInNanos);
            metrics.postIngest(ingestTimeInMillis);
        }
    }

    /**
     * The unique id of this pipeline
     */
    public String getId() {
        return id;
    }

    /**
     * An optional description of what this pipeline is doing to the data gets processed by this pipeline.
     */
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * An optional version stored with the pipeline so that it can be used to determine if the pipeline should be updated / replaced.
     *
     * @return {@code null} if not supplied.
     */
    @Nullable
    public Integer getVersion() {
        return version;
    }

    /**
     * Get the underlying {@link CompoundProcessor} containing the Pipeline's processors
     */
    public CompoundProcessor getCompoundProcessor() {
        return compoundProcessor;
    }

    /**
     * Unmodifiable list containing each processor that operates on the data.
     */
    public List<Processor> getProcessors() {
        return compoundProcessor.getProcessors();
    }

    /**
     * Unmodifiable list containing each on_failure processor that operates on the data in case of
     * exception thrown in pipeline processors
     */
    public List<Processor> getOnFailureProcessors() {
        return compoundProcessor.getOnFailureProcessors();
    }

    /**
     * Flattens the normal and on failure processors into a single list. The original order is lost.
     * This can be useful for pipeline validation purposes.
     */
    public List<Processor> flattenAllProcessors() {
        return compoundProcessor.flattenProcessors();
    }

    /**
     * The metrics associated with this pipeline.
     */
    public IngestMetric getMetrics() {
        return metrics;
    }
}
