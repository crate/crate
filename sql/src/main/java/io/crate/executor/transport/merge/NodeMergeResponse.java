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

package io.crate.executor.transport.merge;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class NodeMergeResponse extends TransportResponse {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final Streamer<?>[] streamers;
    private Object[][] rows;


    public NodeMergeResponse(Streamer<?>[] streamers, Object[][] rows) {
        this.streamers = streamers;
        this.rows = rows;
    }

    public NodeMergeResponse(Streamer<?>[] streamers) {
        this.streamers = streamers;
    }

    public Object[][] rows() {
        return rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        final int numColumns = streamers.length;
        rows = new Object[in.readVInt()][];

        for (int r = 0; r < rows.length; r++) {
            rows[r] = new Object[numColumns];
            for (int c = 0; c < numColumns; c++) {
                rows[r][c] = streamers[c].readValueFrom(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        final int numColumns = streamers.length;

        out.writeVInt(rows.length);
        for (Object[] row : rows) {
            for (int c = 0; c < numColumns; c++) {
                try {
                    streamers[c].writeValueTo(out, row[c]);
                } catch (ClassCastException e) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("streamer pos {} failed. Got {} for streamer {}",
                                e, c, row[c].getClass().getName(), streamers[c]);
                    }
                }
            }
        }
    }
}
