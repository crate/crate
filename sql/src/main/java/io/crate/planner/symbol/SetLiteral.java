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
        setLiterals(valueType, literals);
    }

    /**
     * Initialize an empty SetLiteral.
     * @see SetLiteral#setLiterals Use <code>setLiterals</code> to set values.
     */
    public SetLiteral() {}

    public void setLiterals(DataType valueType, Set<Literal> literals) {
        Preconditions.checkNotNull(valueType);
        Preconditions.checkNotNull(literals);
        this.valueType = valueType;
        this.literals = literals;
        // clear value() in case it has been used already.
        values = null;
    }

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
        if (literals() == null && o == null) {
            return 0;
        } else if (literals() == null && o != null) {
            return -1;
        } else if (literals() != null && o == null) {
            return 1;
        }

        Integer s1 = new Integer(literals().size());
        Integer s2 = new Integer(o.size());
        return s1.compareTo(s2);
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
        DataType dataType = DataType.fromStream(in);
        int numLiterals = in.readVInt();
        if (numLiterals > 0) {
            Set<Literal> literals = new HashSet<>(numLiterals);
            for (int i = 0; i < numLiterals; i++) {
                literals.add((Literal) Literal.fromStream(in));
            }
            setLiterals(dataType, literals);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (literals != null) {
            DataType.toStream(valueType(), out);

            int numLiterals = literals().size();
            out.writeVInt(numLiterals);
            for (Literal literal : literals()) {
                Literal.toStream(literal, out);
            }
        } else {
            out.writeVInt(0);
        }
    }

}
