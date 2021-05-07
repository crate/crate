/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dml.delete;

import io.crate.execution.dml.ShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

public class ShardDeleteRequest extends ShardRequest<ShardDeleteRequest, ShardDeleteRequest.Item> {

    private int skipFromLocation = -1;

    public ShardDeleteRequest(ShardId shardId, UUID jobId) {
        super(shardId, jobId);
    }

    void skipFromLocation(int location) {
        skipFromLocation = location;
    }

    int skipFromLocation() {
        return skipFromLocation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out);
        }
        if (skipFromLocation > -1) {
            out.writeBoolean(true);
            out.writeVInt(skipFromLocation);
        } else {
            out.writeBoolean(false);
        }
    }

    public ShardDeleteRequest(StreamInput in) throws IOException {
        super(in);
        int numItems = in.readVInt();
        if (numItems > 0) {
            items = new ArrayList<>(numItems);
            for (int i = 0; i < numItems; i++) {
                items.add(new ShardDeleteRequest.Item(in));
            }
        }
        if (in.readBoolean()) {
            skipFromLocation = in.readVInt();
        }
    }

    public static class Item extends ShardRequest.Item {

        protected Item(StreamInput in) throws IOException {
            super(in);
        }

        public Item(String id) {
            super(id);
        }
    }
}
