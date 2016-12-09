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

package io.crate.metadata;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import io.crate.core.collections.Collectors;
import io.crate.types.AnyType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UndefinedType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class Signature extends ForwardingList<DataType> {

    public static final List<Signature> SIGNATURES_SINGLE_NUMERIC = DataTypes.NUMERIC_PRIMITIVE_TYPES.stream()
        .map(Signature::new)
        .collect(Collectors.toImmutableList());
    public static final List<Signature> SIGNATURES_SINGLE_ALL = DataTypes.ALL_TYPES.stream()
        .map(Signature::new)
        .collect(Collectors.toImmutableList());
    public static final List<Signature> SIGNATURES_SINGLE_ANY = ImmutableList.of(new Signature(AnyType.INSTANCE));
    public static final List<Signature> SIGNATURES_ALL_PAIRS_OF_SAME = DataTypes.ALL_TYPES.stream()
        .map(dt -> new Signature(dt, dt))
        .collect(Collectors.toImmutableList());

    /**
     * Build signatures of paired combinations of all given types including same types e.g. (string, string)
     */
    public static List<Signature> pairedCombinationsOf(Collection<DataType> dataTypes) {
        ImmutableList.Builder<Signature> builder = ImmutableList.builder();
        for (DataType leftType : dataTypes) {
            for (DataType rightType : dataTypes) {
                builder.add(new Signature(leftType, rightType));
            }
        }
        return builder.build();
    }

    private final ImmutableList<DataType> list;

    /**
     * Defines which data types can be repeated unlimited to match this signature.
     * The value will be used to build a subList of data types which will be used as a repeatable sequence.
     *
     * Example:
     *
     *  list =                      [string, string, long]
     *  varArgsStartingPosition =   1
     *
     *     -> variable sequence =   [string, long]
     *
     * Matching lists:
     *
     *  [string, string, long]
     *  [string, string, long, string, long]
     *  ...
     *
     * If the value is -1, no variable argument matching is done.
     */
    private final int varArgsStartingPosition;

    public Signature(Collection<? extends DataType> c) {
        this(-1, c);
    }

    public Signature(int varArgsStartingPosition, Collection<? extends DataType> c) {
        assert varArgsStartingPosition < c.size() : "varArgs starting position exceeds args definition";
        this.varArgsStartingPosition = varArgsStartingPosition;
        list = ImmutableList.copyOf(c);
    }

    public Signature(DataType... elements) {
        this(-1, elements);
    }

    public Signature(int varArgsStartingPosition, DataType... elements) {
        assert varArgsStartingPosition < elements.length : "varArgs starting position exceeds args definition";
        this.varArgsStartingPosition = varArgsStartingPosition;
        list = ImmutableList.copyOf(elements);
    }

    /**
     * Checks all given {@link DataType} elements against own list.
     * Also both lists must have the same size unless {@link #varArgsStartingPosition} is defined (greater than -1).
     * If {@link #varArgsStartingPosition} is defined, a sublist using this value as a fromIndex can occur
     * multiple times for a valid match.
     */
    public boolean matches(List<DataType> argumentTypes) {
        ListIterator<DataType> signatureIt = listIterator();
        ListIterator<DataType> givenSignatureIt = argumentTypes.listIterator();
        while (signatureIt.hasNext() && givenSignatureIt.hasNext()) {
            if (!typeEqualsOrUndefined(givenSignatureIt.next(), signatureIt.next())) {
                return false;
            }
        }
        if (varArgsStartingPosition == -1) {
            return !(signatureIt.hasNext() || givenSignatureIt.hasNext());
        } else {
            if (!signatureIt.hasNext() && !givenSignatureIt.hasNext()) {
                // no optional var arg, signature matched
                return true;
            }
            List<DataType> varArgs = this.subList(varArgsStartingPosition, size());
            Iterator<DataType>  varArgsIt = varArgs.iterator();
            while (givenSignatureIt.hasNext()) {
                if (!varArgsIt.hasNext()) {
                    varArgsIt = varArgs.iterator();
                }
                if (!typeEqualsOrUndefined(givenSignatureIt.next(), varArgsIt.next())) {
                    return false;
                }
            }
            return !(varArgsIt.hasNext() || givenSignatureIt.hasNext());
        }
    }

    private boolean typeEqualsOrUndefined(DataType givenType, DataType expectedType) {
        return givenType.id() == UndefinedType.ID || expectedType.equals(givenType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof List)) return false;
        List otherList = (List) o;
        return list.equals(otherList);
    }

    @Override
    protected List<DataType> delegate() {
        return list;
    }
}
