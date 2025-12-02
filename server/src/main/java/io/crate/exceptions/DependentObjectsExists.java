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
import java.util.Locale;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.rest.action.HttpErrorStatus;

public class DependentObjectsExists extends ElasticsearchException {

    public DependentObjectsExists(String entity, String name) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot drop %s \"%s\" because other objects depend on it",
            entity,
            name
        ));
    }

    public DependentObjectsExists(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public PGErrorStatus pgErrorStatus() {
        return PGErrorStatus.DEPENDENT_OBJECTS_STILL_EXIST;
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.DEPENDENT_OBJECTS_STILL_EXIST;
    }
}
