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

package io.crate.metadata.functions;

import javax.annotation.Nullable;
import java.util.Objects;

public class TypeVariableConstraint {

    public static TypeVariableConstraint typeVariable(String name) {
        return new TypeVariableConstraint(name, false, false, null);
    }

    private final String name;
    private final boolean comparableRequired;
    private final boolean orderableRequired;
    @Nullable
    private final String variadicBound;

    public TypeVariableConstraint(String name,
                                  boolean comparableRequired,
                                  boolean orderableRequired,
                                  @Nullable String variadicBound) {
        this.name = name;
        this.comparableRequired = comparableRequired;
        this.orderableRequired = orderableRequired;
        this.variadicBound = variadicBound;
    }

    public String getName() {
        return name;
    }

    public boolean isComparableRequired() {
        return comparableRequired;
    }

    public boolean isOrderableRequired() {
        return orderableRequired;
    }

    @Nullable
    public String getVariadicBound() {
        return variadicBound;
    }


    @Override
    public String toString() {
        String value = name;
        if (comparableRequired) {
            value += ":comparable";
        }
        if (orderableRequired) {
            value += ":orderable";
        }
        if (variadicBound != null) {
            value += ":" + variadicBound + "<*>";
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeVariableConstraint that = (TypeVariableConstraint) o;
        return comparableRequired == that.comparableRequired &&
               orderableRequired == that.orderableRequired &&
               Objects.equals(name, that.name) &&
               Objects.equals(variadicBound, that.variadicBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, comparableRequired, orderableRequired, variadicBound);
    }
}
