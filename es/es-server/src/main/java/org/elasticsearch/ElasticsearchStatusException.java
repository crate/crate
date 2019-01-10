/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception who's {@link RestStatus} is arbitrary rather than derived. Used, for example, by reindex-from-remote to wrap remote exceptions
 * that contain a status.
 */
public class ElasticsearchStatusException extends ElasticsearchException {
    private final RestStatus status;

    /**
     * Build the exception with a specific status and cause.
     */
    public ElasticsearchStatusException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, cause, args);
        this.status = status;
    }

    /**
     * Build the exception without a cause.
     */
    public ElasticsearchStatusException(String msg, RestStatus status, Object... args) {
        this(msg, status, null, args);
    }

    /**
     * Read from a stream.
     */
    public ElasticsearchStatusException(StreamInput in) throws IOException {
        super(in);
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        RestStatus.writeTo(out, status);
    }

    @Override
    public final RestStatus status() {
        return status;
    }
}
