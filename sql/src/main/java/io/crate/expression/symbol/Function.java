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

package io.crate.expression.symbol;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.ExplainLeaf;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Function extends Symbol implements Cloneable {

    private final List<Symbol> arguments;
    private final FunctionInfo info;

    public Function(StreamInput in) throws IOException {
        info = new FunctionInfo();
        info.readFrom(in);
        arguments = ImmutableList.copyOf(Symbols.listFromStream(in));
    }

    public Function(FunctionInfo info, List<Symbol> arguments) {
        Preconditions.checkNotNull(info, "function info is null");
        Preconditions.checkArgument(arguments.size() == info.ident().argumentTypes().size(),
            "number of arguments must match the number of argumentTypes of the FunctionIdent");
        this.info = info;
        this.arguments = ImmutableList.copyOf(arguments);
    }

    public List<Symbol> arguments() {
        return arguments;
    }

    public FunctionInfo info() {
        return info;
    }

    @Override
    public DataType valueType() {
        return info.returnType();
    }

    @Override
    public boolean canBeCasted() {
        return info.type() == FunctionInfo.Type.SCALAR || info.type() == FunctionInfo.Type.AGGREGATE;
    }

    @Override
    public Symbol cast(DataType newDataType, boolean tryCast) {
        if (newDataType instanceof CollectionType && info.ident().name().equals(ArrayFunction.NAME)) {
            /* We treat _array(...) in a special way since it's a value constructor and no regular function
             * This allows us to do proper type inference for inserts/updates where there are assignments like
             *
             *      some_array = [?, ?]
             * or
             *      some_array = array_cat([?, ?], [1, 2])
             */
            return castArrayElements(newDataType, tryCast);
        } else {
            return super.cast(newDataType, tryCast);
        }
    }

    private Symbol castArrayElements(DataType newDataType, boolean tryCast) {
        DataType<?> innerType = ((CollectionType) newDataType).innerType();
        ArrayList<Symbol> newArgs = new ArrayList<>(arguments.size());
        for (Symbol arg : arguments) {
            newArgs.add(arg.cast(innerType, tryCast));
        }
        return new Function(
            new FunctionInfo(new FunctionIdent(info.ident().name(), Symbols.typeView(newArgs)), newDataType),
            newArgs
        );
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FUNCTION;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFunction(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
        Symbols.toStream(arguments, out);
    }

    @Override
    public String representation() {
        String name = info.ident().name();
        if (name.startsWith("op_") && arguments.size() == 2) {
            return arguments.get(0).representation()
                   + " " + name.substring(3).toUpperCase(Locale.ENGLISH)
                   + " " + arguments.get(1).representation();
        }
        return name + '(' + ExplainLeaf.printList(arguments) + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Function function = (Function) o;

        if (!arguments.equals(function.arguments)) return false;
        return info.equals(function.info);
    }

    @Override
    public int hashCode() {
        int result = arguments.hashCode();
        result = 31 * result + info.hashCode();
        return result;
    }

    @Override
    public Function clone() {
        return new Function(this.info, this.arguments);
    }
}
