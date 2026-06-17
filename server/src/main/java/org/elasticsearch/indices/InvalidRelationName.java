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

package org.elasticsearch.indices;

import java.io.IOException;
import java.util.Locale;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.exceptions.UnscopedException;
import io.crate.rest.action.HttpErrorStatus;

public class InvalidRelationName extends ElasticsearchException implements UnscopedException {

    public InvalidRelationName(String tableName, String desc) {
        super(String.format(Locale.ENGLISH, "Relation name '%s' is invalid. %s", tableName, desc));
        setIndex(tableName);
    }

    public InvalidRelationName(String tableName, Throwable cause) {
        super(String.format(Locale.ENGLISH, "Relation name '%s' is invalid.", tableName), cause);
        setIndex(tableName);
    }

    public InvalidRelationName(StreamInput in) throws IOException {
        super(in);
    }

    public String tableName() {
        return getIndex().getName();
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.RELATION_INVALID_NAME;
    }
}
