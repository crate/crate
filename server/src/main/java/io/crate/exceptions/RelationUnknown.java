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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.Lists;
import io.crate.metadata.RelationName;
import io.crate.rest.action.HttpErrorStatus;
import io.crate.sql.Identifiers;

public class RelationUnknown extends ElasticsearchException implements ResourceUnknownException, TableScopeException {

    private final RelationName relationName;

    public static RelationUnknown of(String relation, List<String> candidates) {
        switch (candidates.size()) {
            case 0:
                return new RelationUnknown(relation);

            case 1: {
                var name = RelationName.fromIndexName(relation);
                var msg = "Relation '" + relation + "' unknown. Maybe you meant '" + Identifiers.quoteIfNeeded(candidates.get(0)) + "'";
                return new RelationUnknown(name, msg);
            }
            default: {
                var name = RelationName.fromIndexName(relation);
                var msg = "Relation '" + relation + "' unknown. Maybe you meant one of: "
                          + String.join(", ", Lists.map(candidates, Identifiers::quoteIfNeeded));
                return new RelationUnknown(name, msg);
            }
        }
    }

    public RelationUnknown(String tableName, Throwable e) {
        super(String.format(Locale.ENGLISH, "Relation '%s' unknown", tableName), e);
        this.relationName = RelationName.fromIndexName(tableName);
    }

    public RelationUnknown(String tableName) {
        this(RelationName.fromIndexName(tableName), String.format(Locale.ENGLISH, "Relation '%s' unknown", tableName));
    }

    public RelationUnknown(RelationName relationName) {
        this(relationName, String.format(Locale.ENGLISH, "Relation '%s' unknown", relationName));
    }

    private RelationUnknown(RelationName relationName, String errorMessage) {
        super(errorMessage);
        this.relationName = relationName;
    }

    public RelationUnknown(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
    }

    @Override
    public Collection<RelationName> getTableIdents() {
        return Collections.singletonList(relationName);
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.RELATION_UNKNOWN;
    }
}
