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

package io.crate.operation.collect;

import java.io.IOException;

/**
 * Throw this exception inside a {@link org.apache.lucene.search.Collector#collect} in order to
 * abort the collection. Throwing a {@link org.apache.lucene.search.CollectionTerminatedException}
 * will cause the IndexSearcher to stop collecting the current leaf (segment) but continuing with
 * the next leaf (segment).
 */
public class CollectionAbortedException extends IOException {

    public CollectionAbortedException() {
    }

    public CollectionAbortedException(Throwable cause) {
        super(cause);
    }
}
