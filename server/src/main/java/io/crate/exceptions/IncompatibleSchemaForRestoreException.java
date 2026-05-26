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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotRestoreException;

import io.crate.metadata.RelationName;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.rest.action.HttpErrorStatus;

/**
 * Thrown by RestoreService when a snapshot's table schema is incompatible
 * with the existing target relation - column types, OIDs, PK, CLUSTERED BY,
 * PARTITIONED BY, etc. Modeled on {@link RelationAlreadyExists}: HTTP 409
 * (ConflictException), reports the offending relation (TableScopeException),
 * SQLSTATE 42P17 invalid_object_definition (matches PG ATTACH PARTITION).
 */
public class IncompatibleSchemaForRestoreException
        extends SnapshotRestoreException
        implements ConflictException, TableScopeException {

    private static final String MESSAGE_TMPL = "cannot restore relation [%s]: %s";

    private final RelationName relationName;

    public IncompatibleSchemaForRestoreException(Snapshot snapshot,
                                                 RelationName relationName,
                                                 String detail) {
        super(snapshot, String.format(Locale.ENGLISH, MESSAGE_TMPL, relationName.fqn(), detail));
        this.relationName = relationName;
    }

    public IncompatibleSchemaForRestoreException(StreamInput in) throws IOException {
        super(in);
        this.relationName = new RelationName(in);
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
        return HttpErrorStatus.RESTORE_SCHEMA_INCOMPATIBLE;
    }

    @Override
    public PGErrorStatus pgErrorStatus() {
        return PGErrorStatus.INVALID_OBJECT_DEFINITION;
    }
}
