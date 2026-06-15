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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;

public final class Regtype implements Comparable<Regtype>, Writeable {
    private final int oid;

    public Regtype(int oid) throws IllegalArgumentException {
        this.oid = oid;
    }

    public static Regtype fromName(String name) throws IllegalArgumentException {
        PGType<?> pgType = PGTypes.getByTypName(name);
        return new Regtype(pgType.oid());
    }

    public Regtype(StreamInput in) throws IOException {
        this.oid = in.readInt();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(oid);
    }

    public int oid() {
        return oid;
    }

    public String name() {
        PGType<?> pgType = PGTypes.getByOid(oid);
        if (pgType == null) {
            return String.valueOf(oid);
        } else {
            return pgType.typName();
        }
    }

    @Override
    public int compareTo(Regtype o) {
        return Integer.compare(oid, o.oid);
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public int hashCode() {
        return oid;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Regtype other = (Regtype) obj;
        return oid == other.oid;
    }

}
