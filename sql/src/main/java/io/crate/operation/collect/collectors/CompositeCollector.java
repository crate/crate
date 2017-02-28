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

package io.crate.operation.collect.collectors;

import io.crate.data.BatchIterator;
import io.crate.data.CompositeBatchIterator;
import io.crate.operation.collect.BatchIteratorBuilder;
import io.crate.operation.collect.BatchIteratorCollector;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.RowReceiver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompositeCollector {

    public static CrateCollector newInstance(Collection<? extends BatchIteratorBuilder> builders, RowReceiver rowReceiver) {
        List<BatchIterator> batchIterators = new ArrayList<>();
        for (BatchIteratorBuilder builder : builders) {
            batchIterators.add(builder.build());
        }
        return new BatchIteratorCollector(
            new CompositeBatchIterator(batchIterators.toArray(new BatchIterator[0])), rowReceiver);
    }
}
