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

package io.crate.execution.jobs;

import io.crate.Streamer;
import io.crate.data.Bucket;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class BucketReceiverFactory {

    public enum Type {
        MERGE_BUCKETS
    }

    static PageBucketReceiver createMergeBucketsReceiver(Logger logger,
                                                         String nodeName,
                                                         int phaseId,
                                                         Streamer<?>[] streamers,
                                                         Set<Integer> bucketIds,
                                                         Set<Integer> exhaustedBucketIds,
                                                         Map<Integer, Bucket> bucketsByIds,
                                                         Map<Integer, PageResultListener> listenersByBucketIds,
                                                         Consumer<Throwable> onIllegalStateKillOperation,
                                                         Runnable onTriggerConsumer,
                                                         Consumer<Map<Integer, Bucket>> onMergeBuckets,
                                                         Throwable[] lastEncounteredThrowableContainer,
                                                         int numBuckets) {

        return new MergeBucketsReceiver(logger, nodeName, phaseId, streamers, bucketIds, exhaustedBucketIds, bucketsByIds,
            listenersByBucketIds, onIllegalStateKillOperation, onTriggerConsumer, onMergeBuckets, lastEncounteredThrowableContainer,
            numBuckets);
    }

}
