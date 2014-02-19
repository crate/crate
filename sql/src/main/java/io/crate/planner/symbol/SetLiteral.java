/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.planner.symbol;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.cratedb.DataType;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

public class SetLiteral extends Literal<Set<?>, SetLiteral> {

    private DataType itemType;
    private DataType valueType;
    private Set<?> values;

    public static final SymbolFactory<SetLiteral> FACTORY = new SymbolFactory<SetLiteral>() {
        @Override
        public SetLiteral newInstance() {
            return new SetLiteral();
        }
    };

    private ImmutableSet<Literal> literals;

    public static SetLiteral fromLiterals(DataType itemType, Set<Literal> literals) {
        ImmutableSet.Builder<Object> builder = ImmutableSet.<Object>builder();
        for (Literal literal : literals) {
            assert literal.valueType() == itemType :
                    String.format("Literal type: %s does not match item type: %s", literal.valueType(), itemType);
            builder.add(literal.value());
        }
        return new SetLiteral(itemType, builder.build());
    }

    /**
     * returns the intersection of both of the sets values and tries to reuse
     * the according existing SetLiteral if possible
     *
     * @param other the set with new values
     * @return a set containing all values in common
     */
    public SetLiteral intersection(SetLiteral other) {
        Preconditions.checkArgument(itemType == other.itemType);
        if (values.size() == 0) {
            return this;
        } else if (other.values.size() == 0) {
            return other;
        } else if (values.equals(other.values)) {
            return this;
        }
        return new SetLiteral(itemType, Sets.intersection(values, other.values));
    }

    public SetLiteral(DataType itemType, Set<?> values) {
        assert values != null;
        this.itemType = itemType;
        this.valueType = DataType.SET_TYPES.get(itemType.ordinal());
        this.values = values;
    }

    private SetLiteral() {
    }

    public boolean contains(Literal literal) {
        if (values.size() > 0 && literal.valueType() == itemType) {
            return values.contains(literal.value());
        }
        return false;
    }

    @Override
    public Set<?> value() {
        return values;
    }

    public int size() {
        return values.size();
    }

    public Set<Literal> literals() {
        if (literals == null) {
            literals = ImmutableSet.<Literal>builder().addAll(Iterators.transform(
                    values.iterator(), new com.google.common.base.Function<Object, Literal>() {
                @Nullable
                @Override
                public Literal apply(@Nullable Object input) {
                    return Literal.forType(itemType, input);
                }
            })).build();
        }
        return literals;
    }

    @Override
    public int compareTo(SetLiteral o) {
        // Compare the size of the lists.
        // We do this because it's quite easy to compare Set's in this case.
        // A more sophisticated approach would be as follows:
        // - if the size of set1 and set2 differes, sort both sets ascending
        // - compare each value (s1[i].compateTo(s2[i]). If they are not equal, return the comparison result.

        if (o == null) {
            return 1;
        } else if (this == o) {
            return 0;
        }
        return Integer.compare(values.size(), o.values.size());
    }

    @Override
    public DataType valueType() {
        return valueType;
    }

    public DataType itemType() {
        return itemType;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.SET_LITERAL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitSetLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        itemType = DataType.fromStream(in);
        valueType = DataType.SET_TYPES.get(itemType.ordinal());
        values = (Set<Object>) valueType.streamer().readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataType.toStream(itemType, out);
        valueType.streamer().writeTo(out, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetLiteral literal = (SetLiteral) o;
        if (itemType != literal.itemType) return false;
        if (!values.equals(literal.values)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = values.size();
        result = 31 * result + itemType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("itemType", itemType)
                .add("numValues", values.size())
                .toString();
    }


}
