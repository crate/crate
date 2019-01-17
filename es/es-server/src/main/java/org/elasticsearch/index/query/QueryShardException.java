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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception that is thrown when creating lucene queries on the shard
 */
public class QueryShardException extends ElasticsearchException {

    public QueryShardException(QueryShardContext context, String msg, Object... args) {
        this(context, msg, null, args);
    }

    public QueryShardException(QueryShardContext context, String msg, Throwable cause, Object... args) {
        this(context.getFullyQualifiedIndex(), msg, cause, args);
    }

    /**
     * This constructor is provided for use in unit tests where a
     * {@link QueryShardContext} may not be available
     */
    public QueryShardException(Index index, String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
        setIndex(index);
    }

    public QueryShardException(StreamInput in) throws IOException{
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
