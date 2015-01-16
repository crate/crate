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

package io.crate.executor;

import com.google.common.base.MoreObjects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class PageInfo {

    private final int size;
    private final int position;

    public PageInfo(int position, int size) {
        this.position = position;
        this.size = size;
    }

    public int size() {
        return size;
    }

    public int position() {
        return position;
    }

    public static PageInfo firstPage(int size) {
        return new PageInfo(0, size);
    }

    public PageInfo nextPage(int nextSize) {
        return new PageInfo(position+size, nextSize);
    }

    public PageInfo nextPage() {
        return new PageInfo(position+size, size);
    }

    public static PageInfo fromStream(StreamInput in) throws IOException {
        return new PageInfo(in.readVInt(), in.readVInt());
    }

    public static void toStream(PageInfo pageInfo, StreamOutput out) throws IOException {
        out.writeVInt(pageInfo.position);
        out.writeVInt(pageInfo.size);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("position", position)
                .add("size", size).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PageInfo pageInfo = (PageInfo) o;

        if (position != pageInfo.position) return false;
        if (size != pageInfo.size) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = size;
        result = 31 * result + position;
        return result;
    }
}
