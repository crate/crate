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

package io.crate.planner.plan;

import com.google.common.base.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TopNNode extends PlanNode {


    int[] orderBy;
    boolean[] reverseFlags;
    private int limit;
    private int offset;

    public TopNNode(String id) {
        super(id);
    }

    public TopNNode(String id, int limit, int offset) {
        this(id);
        Preconditions.checkArgument(limit + offset < Integer.MAX_VALUE, "Range too big", limit, offset);
        this.limit = limit;
        this.offset = offset;
    }

    public TopNNode(String id, int limit, int offset, int[] orderBy, boolean[] reverseFlags) {
        this(id, limit, offset);
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;
    }

    public boolean isOrdered() {
        return orderBy != null && orderBy.length > 0;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public int[] orderBy() {
        return orderBy;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitTopNNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        offset = in.readVInt();
        limit = in.readVInt();

        orderBy = new int[in.readVInt()];
        for (int i = 0; i < orderBy.length; i++) {
            orderBy[i] = in.readVInt();
        }

        reverseFlags = new boolean[in.readVInt()];
        for (int i = 0; i < reverseFlags.length; i++) {
            reverseFlags[i] = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(offset);
        out.writeVInt(limit);
        out.writeVInt(orderBy.length);
        for (int i : orderBy) {
            out.writeVInt(i);
        }

        out.writeVInt(reverseFlags.length);
        for (boolean reverseFlag : reverseFlags) {
            out.writeBoolean(reverseFlag);
        }
    }

}
