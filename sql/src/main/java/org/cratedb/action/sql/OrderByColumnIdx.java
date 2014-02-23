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

package org.cratedb.action.sql;

import com.google.common.collect.Ordering;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

@Deprecated
public class OrderByColumnIdx implements Streamable {

    public Integer index;
    private boolean isAsc;
    public Ordering<Comparable> ordering = Ordering.natural();

    public boolean isAsc() {
        return isAsc;
    }

    public void isAsc(boolean isAsc) {
        this.isAsc = isAsc;
        if (isAsc) {
            ordering = Ordering.natural();
        } else {
            ordering = Ordering.natural().reverse();
        }
    }

    public OrderByColumnIdx() {
        // empty ctor for streaming
    }

    public OrderByColumnIdx(int index, boolean isAsc) {
        this.index = index;
        isAsc(isAsc);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readVInt();
        isAsc(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(index);
        out.writeBoolean(isAsc);
    }
}
