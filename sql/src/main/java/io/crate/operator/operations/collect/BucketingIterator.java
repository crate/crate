/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.operations.collect;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * bucketing input rows while iterating
 */
public abstract class BucketingIterator implements Iterable<List<Object[]>> {

    private final List<List<Object[]>> buckets;
    private final Iterable<Object[]> inputRowIterable;
    protected final int numBuckets;

    public BucketingIterator(final int numBuckets, Iterable<Object[]> rowIterable) {
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>(this.numBuckets);
        for (int i = 0; i<this.numBuckets; i++) {
            buckets.add(new ArrayList<Object[]>());
        }
        this.inputRowIterable = rowIterable;
    }

    /**
     * get number of bucket to put this row into
     * @param row input row
     * @return the 0 based index of the bucket to put <code>row</code> into
     */
    protected abstract int getBucket(@Nullable Object[] row);

    @Override
    public Iterator<List<Object[]>> iterator() {
        for (Object[] inputRow : inputRowIterable) {
            buckets.get(getBucket(inputRow)).add(inputRow);
        }
        return buckets.iterator();
    }
}
