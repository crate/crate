/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.sql.tree;

import javax.annotation.Nullable;

public class DataTypeParameter extends DataTypeSignature {

    @Nullable
    private final Integer numericalValue;

    @Nullable
    private final String identifier;

    @Nullable
    private final DataTypeSignature dataTypeSignature;

    public DataTypeParameter(@Nullable Integer numericalValue,
                             @Nullable String identifier,
                             @Nullable DataTypeSignature dataTypeSignature) {
        this.numericalValue = numericalValue;
        this.identifier = identifier;
        this.dataTypeSignature = dataTypeSignature;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDataTypeParameter(this, context);
    }

    @Nullable
    public Integer numericalValue() {
        return numericalValue;
    }

    @Nullable
    public String identifier() {
        return identifier;
    }

    @Nullable
    public DataTypeSignature typeSignature() {
        return dataTypeSignature;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public String toString() {
        return null;
    }
}
