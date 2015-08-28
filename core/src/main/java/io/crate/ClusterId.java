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

package io.crate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.UUID;

public class ClusterId implements Streamable {

    private UUID uuid;

    public ClusterId() {
        this.uuid = UUID.randomUUID();
    }

    public ClusterId(String uuid) {
        this.uuid = UUID.fromString(uuid);
    }

    public UUID value() {
        return uuid;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        long msb = in.readVLong();
        long lsb = in.readVLong();
        uuid = new UUID(msb, lsb);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(uuid.getMostSignificantBits());
        out.writeVLong(uuid.getLeastSignificantBits());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterId that = (ClusterId) o;

        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return uuid != null ? uuid.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Cluster [" + uuid.toString() + "]";
    }
}
