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

package io.crate.session;

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationInfo;
import io.crate.types.DataType;

/**
 * Encapsulates the result of a DescribePortal or DescribeParameter message.
 */
public class DescribeResult {

    @Nullable
    private final List<Symbol> fields;
    private final DataType<?>[] parameters;
    private final RelationInfo relation;

    DescribeResult(DataType<?>[] parameters,
                   @Nullable List<Symbol> fields,
                   @Nullable RelationInfo relation) {
        this.fields = fields;
        this.parameters = parameters;
        this.relation = relation;
    }

    @Nullable
    public List<Symbol> getFields() {
        return fields;
    }

    /**
     * Returns the described parameters in sorted order ($1, $2, etc.)
     * @return An array containing the parameters, or null if they could not be obtained.
     */
    public DataType<?>[] getParameters() {
        return parameters;
    }

    /**
     * Relation/Table of the query if the query operates directly on a table.
     *
     * `null` if the query operates on a virtual table or involves joins.
     **/
    @Nullable
    public RelationInfo relation() {
        return relation;
    }
}
