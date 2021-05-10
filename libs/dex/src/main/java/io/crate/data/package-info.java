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
 * Package with data structures for the execution engine / data processing.
 *
 * The main components are:
 *
 *  - {@link io.crate.data.BatchIterator} which provides access to data
 *  - {@link io.crate.data.RowConsumer} consumes data from a BatchIterator
 *
 * BatchIterators come in different forms:
 *
 *  - "Source" BatchIterators: These expose data from either in-memory, disk or network
 *    (Examples are LuceneBatchIterator, {@link io.crate.data.InMemoryBatchIterator} (Iterable backed))
 *
 *  - "Projecting" BatchIterators: These wrap another BatchIterator and transform their data.
 *    There are multiple sub-types:
 *
 *      - "Synchronous/Forwarding": transformation is mostly done on a row per row basis.
 *      Examples include Filtering-, Limiting-, SkippingBatchIterator.
 *
 *      - "Collecting": BatchIterators which need to consume all of the source BatchIterator
 *      in order to produce a new result.
 *      Examples include aggregation, sorting, grouping. See {@link io.crate.data.CollectingBatchIterator}
 *
 *      - Batch/Bulk collecting with async operations:
 *          Similar to the {@link io.crate.data.CollectingBatchIterator}, but instead of consuming the whole source
 *          in one pass, these consume it in batches - after each batch it invokes an async operation, after which
 *          it will continue with the consumption.
 *      Examples include the fetch-operation.
 *      See {@link io.crate.data.AsyncOperationBatchIterator}
 *
 *
 */
package io.crate.data;
