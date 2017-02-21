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

package io.crate.operation.projectors;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.function.Function;

public class MergeCountProjector extends AbstractProjector {

    private long sum;

    @Override
    public Result setNextRow(Row row) {
        Long count = (Long) row.get(0);
        sum += count;
        return Result.CONTINUE;
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        downstream.setNextRow(new Row1(sum));
        downstream.finish(RepeatHandle.UNSUPPORTED);
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }

    @Nullable
    @Override
    public Function<BatchIterator, Tuple<BatchIterator, RowReceiver>> batchIteratorProjection() {
        return bi -> new Tuple<>(CollectingBatchIterator.summingLong(bi), downstream);
    }
}
