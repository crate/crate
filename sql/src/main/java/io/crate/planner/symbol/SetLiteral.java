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

import org.cratedb.DataType;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SetLiteral extends Literal<Set<Object>, Set<Literal>> {

    private Set<Object> values;
    private Set<Literal> literals;

    private DataType valueType;

    public static final SymbolFactory<SetLiteral> FACTORY = new SymbolFactory<SetLiteral>() {
        @Override
        public SetLiteral newInstance() {
            return new SetLiteral();
        }
    };

    public SetLiteral(DataType valueType, Set<Literal> literals) {
        Preconditions.checkNotNull(valueType);
        Preconditions.checkNotNull(literals);
        this.valueType = valueType;
        this.literals = literals;
    }

    private SetLiteral() {}

    public boolean contains(Literal literal) {
        return literals.contains(literal);
    }

    @Override
    public Set<Object> value() {
        if (values == null) {
            values = new HashSet<>(literals.size());
            for (Literal l : literals) {
                values.add(l.value());
            }
        }
        return values;
    }

    public Set<Literal> literals() {
        return literals;
    }

    @Override
    public int compareTo(Set<Literal> o) {
        // Compare the size of the lists.
        // We do this because it's quite easy to compare Set's in this case.
        // A more sophisticated approach would be as follows:
        // - if the size of set1 and set2 differes, sort both sets ascending
        // - compare each value (s1[i].compateTo(s2[i]). If they are not equal, return the comparison result.
        if (literals() == null && o == null) {
            return 0;
        } else if (literals() == null && o != null) {
            return -1;
        } else if (literals() != null && o == null) {
            return 1;
        }

        return Integer.compare(literals().size(), o.size());
    }

    @Override
    public DataType valueType() {
        return valueType;
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
        valueType = DataType.fromStream(in);
        int numLiterals = in.readVInt();
        if (numLiterals > 0) {
            literals = new HashSet<>(numLiterals);
            for (int i = 0; i < numLiterals; i++) {
                literals.add((Literal) Literal.fromStream(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataType.toStream(valueType(), out);

        // TODO: we could just write the Set<Object> values.
        int numLiterals = literals().size();
        out.writeVInt(numLiterals);
        for (Literal literal : literals()) {
            Literal.toStream(literal, out);
        }
    }

}
