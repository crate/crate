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

package io.crate.types;

import java.io.IOException;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.exceptions.InvalidRelationName;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.SessionSettings;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedNameReference;

/**
 * Type that encapsulates the name and oid of a relation.
 **/
public final class RegclassType extends DataType<Regclass> implements Streamer<Regclass> {

    public static final RegclassType INSTANCE = new RegclassType();
    public static final int ID = 23;

    private RegclassType() {}

    @Override
    public int compare(Regclass o1, Regclass o2) {
        return o1.compareTo(o2);
    }

    @Override
    public Regclass readValueFrom(StreamInput in) throws IOException {
        return in.readOptionalWriteable(Regclass::new);
    }

    @Override
    public void writeValueTo(StreamOutput out, Regclass v) throws IOException {
        out.writeOptionalWriteable(v);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.REGCLASS;
    }

    @Override
    public String getName() {
        return "regclass";
    }

    @Override
    public Streamer<Regclass> streamer() {
        return this;
    }

    @Override
    public Regclass sanitizeValue(Object value) {
        if (value == null) {
            return null;
        }
        return (Regclass) value;
    }

    @Override
    public Regclass explicitCast(Object value, SessionSettings sessionSettings) throws IllegalArgumentException, ClassCastException {
        return cast(value, sessionSettings.currentSchema());
    }

    @Override
    public Regclass implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return cast(value, CoordinatorTxnCtx.systemTransactionContext().sessionSettings().currentSchema());
    }

    private Regclass cast(Object value, String currentSchema) {
        if (value == null) {
            return null;
        }
        if (value instanceof Regclass regclass) {
            return regclass;
        }
        if (value instanceof Integer num) {
            return new Regclass(num.intValue(), value.toString());
        }
        if (value instanceof Long num) {
            if (num > Integer.MAX_VALUE || num < Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                    value + " is outside of `int` range and cannot be cast to the regclass type");
            }
            return new Regclass(num.intValue(), value.toString());
        }
        if (value instanceof String s) {
            try {
                var qualifiedNameReference = (QualifiedNameReference) SqlParser.createExpression(s);
                var relationName = RelationName.of(qualifiedNameReference.getName(), currentSchema);
                return Regclass.fromRelationName(relationName);
            } catch (ParsingException e) {
                throw new InvalidRelationName(s, e);
            }
        }
        throw new ClassCastException("Can't cast '" + value + "' to " + getName());
    }

    @Override
    public long valueBytes(Regclass value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        return RamUsageEstimator.sizeOf(value.name()) + RamUsageEstimator.sizeOf(value.oid());
    }
}
