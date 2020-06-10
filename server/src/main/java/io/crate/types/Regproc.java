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

package io.crate.types;

import io.crate.metadata.pgcatalog.OidHash;

import javax.annotation.Nonnull;
import java.util.Objects;

public class Regproc {

    private final int oid;
    private final String name;

    public static Regproc of(@Nonnull String name) {
        return new Regproc(OidHash.functionOid(name), name);
    }

    public static Regproc of(int oid, @Nonnull String name) {
        // To match PostgreSQL behavior 1:1 this would need to lookup the
        // function name by oid and fallback to using the oid as name if there is
        // no match.
        // It looks like for compatibility with clients it is good enough
        // to not mirror this behavior.
        return new Regproc(oid, name);
    }

    private Regproc(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    public int oid() {
        return oid;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Regproc regproc = (Regproc) o;
        return oid == regproc.oid &&
               Objects.equals(name, regproc.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, name);
    }

    @Override
    public String toString() {
        return name;
    }
}
