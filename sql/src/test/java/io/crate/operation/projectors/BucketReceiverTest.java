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

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.concurrent.CompletionListenable;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.testing.RowCollectionBucket;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BucketReceiverTest {

    @Test
    public void testEmitter() throws Exception {

        CollectingBucketReceiver c = new CollectingBucketReceiver();
        LimitProjector lp = new LimitProjector(100, c);
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ArrayList<Row> l = new ArrayList<Row>();
            for (int j = 0; j < 42; j++) {
                l.add(new Row1(j));
            }
            buckets.add(new RowCollectionBucket(l));
        }

        ExampleBucketEmitter emitter = new ExampleBucketEmitter(lp, buckets);
        emitter.run();
        emitter.completionFuture().get();
        assertThat(c.count(), is(100));

    }


    static class CollectingBucketReceiver implements BucketReceiver {

        final List<Bucket> received = new ArrayList<>();

        @Override
        public int rowsNeeded() {
            return -1;
        }

        @Override
        public ListenableFuture<?> setBucket(Bucket bucket, boolean isLast) {
            received.add(bucket);
            return Futures.immediateFuture(null);
        }

        public int count(){
            int c = 0;
            for (Bucket rows : received) {
                c+=rows.size();
            }
            return c;
        }

    }

    static class LimitProjector implements BucketReceiver {

        private int limit;
        private final BucketReceiver downstream;

        public LimitProjector(int limit, BucketReceiver downstream) {
            this.limit = limit;
            this.downstream = downstream;
        }

        @Override
        public int rowsNeeded() {
            int needed = limit < 0 ? 0 : limit;
            if (downstream.rowsNeeded() >= 0){
                return Math.min(needed, downstream.rowsNeeded());
            }
            return needed;
        }

        private static class LimitedBucket implements Bucket {

            private final int limit;
            private final Bucket wrapped;

            LimitedBucket(int limit, Bucket wrapped) {
                assert limit < wrapped.size();
                this.limit = limit;
                this.wrapped = wrapped;
            }

            @Override
            public int size() {
                return limit;
            }

            @Override
            public Iterator<Row> iterator() {
                return Iterators.limit(wrapped.iterator(), limit);
            }
        }

        @Override
        public ListenableFuture<?> setBucket(Bucket bucket, boolean isLast) {
            assert limit >= 0;
            int needed = rowsNeeded();
            assert needed >= 0;

            limit -= bucket.size();
            if (needed < bucket.size()) {
                bucket = new LimitedBucket(needed, bucket);
                isLast = true;
            } else if (needed == bucket.size()) {
                isLast = true;
            }
            //since we do re-use the input buckets we should wait until the downstream frees the buckets?
            return downstream.setBucket(bucket, isLast);
        }
    }

    static class ProjectorWrapper implements BucketReceiver {

        private final  Projector projector;
        private int rowsNeeded = -1;

        public ProjectorWrapper(Projector projector) {
            this.projector = projector;
        }

        @Override
        public int rowsNeeded() {
            return rowsNeeded;
        }


        @Override
        public ListenableFuture<?> setBucket(Bucket bucket, boolean isLast) {
            boolean stopped = false;
            L: for (Row row : bucket) {
                switch(projector.setNextRow(row)){
                    case CONTINUE:
                        continue;
                    case STOP:
                        stopped = true;
                        break L;
                    default:
                        return Futures.immediateFailedFuture(
                            new UnsupportedOperationException("pause not implemented"));
                }
            }

            if (isLast || stopped){
                projector.finish(RepeatHandle.UNSUPPORTED);
                rowsNeeded = 0;
            }
            return Futures.immediateFuture(null);
        }
    }



    static class ExampleBucketEmitter implements CompletionListenable {

        private final BucketReceiver downstream;
        private final List<Bucket> buckets;
        private final Iterator<Bucket> iter;
        private SettableFuture<?> completionFuture = SettableFuture.create();

        public ExampleBucketEmitter(BucketReceiver downstream, List<Bucket> buckets) {
            this.downstream = downstream;
            this.buckets = buckets;
            this.iter = buckets.iterator();
        }

        public void run() {
            if (iter.hasNext() && downstream.rowsNeeded() != 0) {
                ListenableFuture<?> f = downstream.setBucket(iter.next(), !iter.hasNext());
                Futures.addCallback(f, new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(@Nullable Object result) {
                        run();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        completionFuture.setException(t);
                    }
                });
            } else {
                completionFuture.set(null);
            }
        }

        @Override
        public ListenableFuture<?> completionFuture() {
            return completionFuture;
        }
    }





}
