/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.pgcatalog;

import io.crate.metadata.RelationInfo;
import org.apache.lucene.util.BytesRef;

import static org.apache.lucene.util.StringHelper.murmurhash3_x86_32;

final class OidHash {

    enum Type {
        SCHEMA,
        TABLE,
        VIEW,
        CONSTRAINT
    }

    static int relationOid(RelationInfo relationInfo) {
        Type t = relationInfo.relationType() == RelationInfo.RelationType.VIEW ? Type.VIEW : Type.TABLE;
        BytesRef b = new BytesRef(t.toString() + relationInfo.ident().fqn());
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    static int schemaOid(String name) {
        BytesRef b = new BytesRef(Type.SCHEMA.toString() + name);
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    static int constraintOid(String relationName, String constraintName, String constraintType) {
        BytesRef b = new BytesRef(Type.CONSTRAINT.toString() + relationName + constraintName + constraintType);
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }
}
