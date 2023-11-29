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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractAssert;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.types.DataType;

public class ReferenceAssert extends AbstractAssert<ReferenceAssert, Reference> {

    public ReferenceAssert(Reference reference) {
        super(reference, ReferenceAssert.class);
    }

    public ReferenceAssert hasOid(long expectedOid) {
        assertThat(actual.oid())
            .as("oid")
            .isEqualTo(expectedOid);
        return this;
    }

    public ReferenceAssert hasName(String expectedName) {
        assertThat(actual.column().sqlFqn())
            .as("sqlFqn")
            .isEqualTo(expectedName);
        return this;
    }

    public ReferenceAssert hasPosition(int expectedPosition) {
        assertThat(actual.position())
            .as("position")
            .isEqualTo(expectedPosition);
        return this;
    }

    public ReferenceAssert hasTableIdent(RelationName expectedTableIdent) {
        assertThat(actual.ident().tableIdent())
            .as("tableIdent")
            .isEqualTo(expectedTableIdent);
        return this;
    }

    public ReferenceAssert hasColumnIdent(ColumnIdent expectedColumnIdent) {
        assertThat(actual.column())
            .as("columnIdent")
            .isEqualTo(expectedColumnIdent);
        return this;
    }

    public ReferenceAssert hasType(DataType<?> expectedType) {
        assertThat(actual.valueType())
            .as("type")
            .isEqualTo(expectedType);
        return this;
    }

    public ReferenceAssert hasAnalyzer(String analyzer) {
        isExactlyInstanceOf(IndexReference.class);
        assertThat(((IndexReference) actual).analyzer()).isEqualTo(analyzer);
        return this;
    }

}
