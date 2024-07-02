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
import java.util.Collections;
import java.util.Locale;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;

import io.crate.metadata.RelationName;
import io.crate.rest.action.HttpErrorStatus;

public final class RelationAlreadyExists extends ElasticsearchException implements ConflictException, TableScopeException {

    private static final String MESSAGE_TMPL = "Relation '%s' already exists.";

    private RelationName relationName;

    public RelationAlreadyExists(RelationName relationName) {
        super(String.format(Locale.ENGLISH, MESSAGE_TMPL, relationName));
        this.relationName = relationName;
    }

    public RelationAlreadyExists(RelationName relationName, String message) {
        super(message);
        this.relationName = relationName;
    }

    RelationAlreadyExists(Index index, Throwable e) {
        super(String.format(Locale.ENGLISH, MESSAGE_TMPL, index.getName()), e);
        this.relationName = RelationName.fromIndexName(index.getName());
    }

    public RelationAlreadyExists(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
    }

    @Override
    public Iterable<RelationName> getTableIdents() {
        return Collections.singletonList(relationName);
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY;
    }
}
