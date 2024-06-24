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

import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.OidHash;

import org.jetbrains.annotations.NotNull;
import java.util.Objects;

public class Regproc {

    public static final Regproc REGPROC_ZERO = Regproc.of(0, "-");

    private final int oid;
    private final String name;

    public static Regproc of(@NotNull String name) {
        return new Regproc(
            OidHash.functionOid(Signature.scalar(name, DataTypes.UNDEFINED.getTypeSignature()).withFeature(Scalar.Feature.DETERMINISTIC)),
            name
        );
    }

    public static Regproc of(int functionOid, @NotNull String name) {
        // To match PostgreSQL behavior 1:1 this would need to lookup the
        // function name by oid and fallback to using the oid as name if there is
        // no match.
        // It looks like for compatibility with clients it is good enough
        // to not mirror this behavior.
        return new Regproc(functionOid, name);
    }

    private Regproc(int functionOid, String name) {
        this.oid = functionOid;
        this.name = name;
    }

    public Signature asDummySignature() {
        return Signature.scalar(name, DataTypes.UNDEFINED.getTypeSignature()).withFeature(Scalar.Feature.DETERMINISTIC);
    }

    public int oid() {
        return oid;
    }

    @NotNull
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
        // Such as we cannot lookup the function name by oid and fallback
        // to using the oid as name, we cannot use the name to check the
        // equality of the Regproc objects. Therefore, we use the oid only
        // to check the equality and calculate the hash code.
        Regproc regproc = (Regproc) o;
        return oid == regproc.oid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid);
    }

    @Override
    public String toString() {
        return name;
    }
}
