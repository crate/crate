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

package io.crate.operation.join;

import org.apache.lucene.util.LongBitSet;

import java.util.ArrayList;

/**
 * This BitSet is used to mark matched rows between left and right in {@link NestedLoopOperation}
 *
 * Each bit true if the rows in the respective position are matched and therefore we need
 * a structure capable of holding <pre>long</pre> size of bits so java.util.BitSet cannot be used.
 *
 * We chose to use {@link LongBitSet} from Lucence and add another layer of segments on top in
 * order to further optimize performance by growing the capacity of the backing array by double
 * each time size is reached.
 */
class LuceneLongBitSetWrapper {
    long size = 1024;
    LongBitSet bitSet = new LongBitSet(size);

    void set(long idx) {
        if (idx >= size) {
            size *= 2;
            bitSet = LongBitSet.ensureCapacity(bitSet, size);
        }
        bitSet.set(idx);
    }

    boolean get(long idx) {
        return bitSet.get(idx);
    }
}
