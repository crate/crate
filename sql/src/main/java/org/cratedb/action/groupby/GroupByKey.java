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

package org.cratedb.action.groupby;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Arrays;

public class GroupByKey implements Comparable<GroupByKey> {

    private Object[] keyValue;

    Ordering<Comparable> ordering = Ordering.natural();

    public GroupByKey() {

    }

    public GroupByKey(Object[] keyValue) {
        this.keyValue = keyValue;
    }

    public Object get(int idx) {
        return keyValue[idx];
    }

    public int size() {
        return keyValue.length;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keyValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupByKey)) {
            return super.equals(obj);
        }

        return Arrays.equals(keyValue, ((GroupByKey) obj).keyValue);
    }

    @Override
    public int compareTo(GroupByKey other) {
        assert this.size() == other.size();
        ComparisonChain chain = ComparisonChain.start();

        for (int i = 0; i < this.size(); i++) {
            Object left = this.get(i);
            Object right = other.get(i);

            if (left != null && right != null) {
                chain = chain.compare((Comparable)left, (Comparable)right, ordering);
            } else if (right != null) {
                chain = chain.compare(0, 1);
            } else if (left != null) {
                chain = chain.compare(1, 0);
            } else {
                chain = chain.compare(0, 0);
            }
        }

        return chain.result();
    }

    @Override
    public String toString() {
        return "GroupByKey{" + Arrays.toString(keyValue) + "}";
    }
}
