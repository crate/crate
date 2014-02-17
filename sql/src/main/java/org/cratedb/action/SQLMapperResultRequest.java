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

package org.cratedb.action;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.TransportRequest;

import java.io.*;
import java.util.UUID;

public class SQLMapperResultRequest extends TransportRequest {

    public boolean failed = false;

    // fields below are only set/available on the receiver side.
    final ESLogger logger = Loggers.getLogger(getClass());
    private CacheRecycler cacheRecycler;
    private ReduceJobRequestContext jobStatusContext;

    public UUID contextId;
    public SQLGroupByResult groupByResult;
    public BytesStreamOutput memoryOutputStream = new BytesStreamOutput();
    public ReduceJobContext status;

    public SQLMapperResultRequest() {}
    public SQLMapperResultRequest(ReduceJobRequestContext jobStatusContext) {
        this.jobStatusContext = jobStatusContext;
        cacheRecycler = jobStatusContext.cacheRecycler();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        try {
            contextId = new UUID(in.readLong(), in.readLong());
            status = jobStatusContext.get(contextId);
            failed = in.readBoolean();
            if (!failed){
                if (status == null) {
                    Streams.copy(in, memoryOutputStream);
                } else {
                    groupByResult = SQLGroupByResult.readSQLGroupByResult(
                        status.parsedStatement, cacheRecycler, in);
                }
            }
        } catch (Exception e ) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        try {
            out.writeLong(contextId.getMostSignificantBits());
            out.writeLong(contextId.getLeastSignificantBits());
            out.writeBoolean(failed);
            if (!failed){
                groupByResult.writeTo(out);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}
