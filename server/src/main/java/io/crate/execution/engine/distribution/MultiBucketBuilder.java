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

package io.crate.execution.engine.distribution;

import io.crate.data.Row;
import org.apache.lucene.util.Accountable;

/**
 * Builder used to build one or more buckets
 */
public interface MultiBucketBuilder extends Accountable {

    /**
     * add a row to the page
     */
    void add(Row row);

    /**
     * current number of rows within the page.
     * Will be reset to 0 on each build call.
     */
    int size();

    /**
     * Builds the buckets and writes them into the provided array.
     * The provided array must have size N where N is the number of buckets the page contains.
     * <p>
     * N is usually specified in the constructor of a specific PageBuilder implementation.
     */
    void build(StreamBucket[] buckets);
}
