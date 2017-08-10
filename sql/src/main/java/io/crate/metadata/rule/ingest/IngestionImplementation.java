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

package io.crate.metadata.rule.ingest;

import java.util.Set;

/**
 * Ingestion implementation that usually facilitate the data flow into the system based on the configured ingestion
 * rules.
 */
public interface IngestionImplementation {

    /**
     * Provides a way for the IngestionImplementation to receive the configured ingestion rules after it registered
     * with the {@link IngestionService}.
     * The {@link IngestionService} listens for changes (eg. rules created/dropped, target tables dropped, etc),
     * processes the ingestion rules and feeds them to the IngestionImplementations using this callback.
     * The implementation will receive all rules belonging to its source and should update its ingestion data flow to
     * obey the received rules.
     */
    void applyRules(Set<IngestRule> rules);
}
