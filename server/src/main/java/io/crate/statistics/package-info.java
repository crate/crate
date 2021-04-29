/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * <p>
 *     Contains to generate and expose statistics over the data contained in tables.
 * </p>
 *
 * <p>
 *     The interaction of the components looks as follows
 * </p>
 *
 * <pre>
 * {@code
 *      SQL: ANALYZE command (invoked by user)
 *          AnalyzePlan -> invokes TransportAnalyzeAction
 *
 *      TransportAnalyzeAction
 *          - fetches samples of table from all data nodes
 *          - merges samples
 *          - creates statistics based on the samples
 *          - publishes the statistics to all nodes
 *
 *          - receives statistics and calls TableStats.updateStats
 *
 *
 *       ReservoirSampler
 *          - Contains logic to get sample rows of a table
 *
 *
 *       TableStats
 *          - Singleton providing access to the currently available statistics
 *
 *       TableStatsService
 *          - Periodically invokes a ANALYZE
 * }
 * </pre>
 */
package io.crate.statistics;
