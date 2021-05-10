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

import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.pgcatalog.OidHash;

/**
 * Encapsulates the name and oid of a relation
 */
public final class Regclass implements Comparable<Regclass>, Writeable {

    private final int oid;
    private final String name;


    public static Regclass relationOid(RelationInfo relation) {
        return new Regclass(
            OidHash.relationOid(
                OidHash.Type.fromRelationType(relation.relationType()),
                relation.ident()
            ),
            relation.ident().fqn()
        );
    }

    public static Regclass primaryOid(RelationInfo relation) {
        return new Regclass(
            OidHash.primaryKeyOid(relation.ident(), relation.primaryKey()),
            relation.ident().fqn()
        );
    }

    public static Regclass fromRelationName(String name) {
        return new Regclass(
            OidHash.relationOid(OidHash.Type.TABLE, new RelationName(null, name)),
            name
        );
    }

    public Regclass(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }


    public Regclass(StreamInput in) throws IOException {
        this.oid = in.readInt();
        this.name = in.readString();
    }

    public int oid() {
        return oid;
    }

    public String name() {
        return name;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(oid);
        out.writeString(name);
    }

    @Override
    public int compareTo(Regclass o) {
        return Integer.compare(oid, o.oid);
    }

    @Override
    public String toString() {
        return Integer.toString(oid);
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
        Regclass other = (Regclass) obj;
        return oid == other.oid;
    }
}
