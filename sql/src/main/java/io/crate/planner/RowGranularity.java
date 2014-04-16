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

package io.crate.planner;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * granularity of references
 *
 * order from large-grained to fine-grained:
 *
 * CLUSTER > NODE > SHARD > DOC
 */
public enum RowGranularity {

    // a higher ordinal represents a higher granularity, so order matters here
    CLUSTER,
    NODE,
    SHARD,
    DOC;

    public boolean finerThan(RowGranularity other) {
        return ordinal() > other.ordinal();
    }

    public boolean largerThan(RowGranularity other) {
        return ordinal() < other.ordinal();
    }
    /**
     * return the highest RowGranularity of the two arguments.
     * Higher means finer granularity, e.g. DOC is higher than SHARD
     *
     * @param granularity1
     * @param granularity2
     * @return
     */
    public static RowGranularity max(RowGranularity granularity1, RowGranularity granularity2) {
        if (granularity2.ordinal() > granularity1.ordinal()) {
            return granularity2;
        }
        return granularity1;
    }

    /**
     * return the lowest RowGranularity of the two arguments.
     * Lower means rougher granularity, e.g. CLUSTER is lower than NODE.
     * @param granularity1
     * @param granularity2
     * @return
     */
    public static RowGranularity min(RowGranularity granularity1, RowGranularity granularity2) {
        if (granularity2.ordinal() < granularity1.ordinal()) {
            return granularity2;
        }
        return granularity1;
    }

    public static RowGranularity fromStream(StreamInput in) throws IOException {
        return RowGranularity.values()[in.readVInt()];
    }

    public static void toStream(RowGranularity granularity, StreamOutput out) throws IOException {
        out.writeVInt(granularity.ordinal());
    }
}
