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

package io.crate.exceptions;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.rest.action.HttpErrorStatus;
import io.crate.types.DataType;

public class UserDefinedFunctionUnknownException extends ElasticsearchException implements ResourceUnknownException, SchemaScopeException {

    private final String schema;

    public UserDefinedFunctionUnknownException(String schema, String name, List<DataType<?>> types) {
        super(String.format(Locale.ENGLISH, "Cannot resolve user defined function: '%s.%s(%s)'",
            schema, name, types.stream().map(DataType::getName).collect(Collectors.joining(",")))
        );
        this.schema = schema;
    }

    public UserDefinedFunctionUnknownException(StreamInput in) throws IOException {
        super(in);
        this.schema = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(schema);
    }

    @Override
    public String getSchemaName() {
        return schema;
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.USER_DEFINED_FUNCTION_UNKNOWN;
    }
}
