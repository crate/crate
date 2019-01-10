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

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.script.ScriptService;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

/**
 * A processor implementation may modify the data belonging to a document.
 * Whether changes are made and what exactly is modified is up to the implementation.
 *
 * Processors may get called concurrently and thus need to be thread-safe.
 */
public interface Processor {

    /**
     * Introspect and potentially modify the incoming data.
     */
    IngestDocument execute(IngestDocument ingestDocument) throws Exception;

    /**
     * Gets the type of a processor
     */
    String getType();

    /**
     * Gets the tag of a processor.
     */
    String getTag();

    /**
     * A factory that knows how to construct a processor based on a map of maps.
     */
    interface Factory {

        /**
         * Creates a processor based on the specified map of maps config.
         *
         * @param processorFactories Other processors which may be created inside this processor
         * @param tag The tag for the processor
         * @param config The configuration for the processor
         *
         * <b>Note:</b> Implementations are responsible for removing the used configuration keys, so that after
         * creating a pipeline ingest can verify if all configurations settings have been used.
         */
        Processor create(Map<String, Processor.Factory> processorFactories, String tag,
                         Map<String, Object> config) throws Exception;
    }

    /**
     * Infrastructure class that holds services that can be used by processor factories to create processor instances
     * and that gets passed around to all {@link org.elasticsearch.plugins.IngestPlugin}s.
     */
    class Parameters {

        /**
         * Useful to provide access to the node's environment like config directory to processor factories.
         */
        public final Environment env;

        /**
         * Provides processors script support.
         */
        public final ScriptService scriptService;

        /**
         * Provide analyzer support
         */
        public final AnalysisRegistry analysisRegistry;

        /**
         * Allows processors to read headers set by {@link org.elasticsearch.action.support.ActionFilter}
         * instances that have run prior to in ingest.
         */
        public final ThreadContext threadContext;

        public final LongSupplier relativeTimeSupplier;

        public final IngestService ingestService;

        /**
         * Provides scheduler support
         */
        public final BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler;

        public Parameters(Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry,  ThreadContext threadContext,
                          LongSupplier relativeTimeSupplier, BiFunction<Long, Runnable, ScheduledFuture<?>> scheduler,
            IngestService ingestService) {
            this.env = env;
            this.scriptService = scriptService;
            this.threadContext = threadContext;
            this.analysisRegistry = analysisRegistry;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            this.ingestService = ingestService;
        }

    }
}
