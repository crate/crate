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
import io.crate.expression.scalar.arithmetic.ArrayFunction;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.ExplainLeaf;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class Function extends Symbol implements Cloneable {

    private final List<Symbol> arguments;
    private final FunctionInfo info;
    @Nullable
    private final Symbol filter;

    public Function(StreamInput in) throws IOException {
        info = new FunctionInfo(in);
        if (in.getVersion().onOrAfter(Version.V_4_1_0)) {
            filter = Symbols.nullableFromStream(in);
        } else {
            filter = null;
        }
        arguments = List.copyOf(Symbols.listFromStream(in));
    }

    public Function(FunctionInfo info, List<Symbol> arguments) {
        this(info, arguments, null);
    }

    public Function(FunctionInfo info, List<Symbol> arguments, Symbol filter) {
        Preconditions.checkNotNull(info, "function info is null");
        Preconditions.checkArgument(arguments.size() == info.ident().argumentTypes().size(),
            "number of arguments must match the number of argumentTypes of the FunctionIdent");
        this.info = info;
        this.arguments = List.copyOf(arguments);
        this.filter = filter;
    }

    public List<Symbol> arguments() {
        return arguments;
    }

    public FunctionInfo info() {
        return info;
    }

    @Nullable
    public Symbol filter() {
        return filter;
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
    public boolean isValueSymbol() {
        for (Symbol argument : arguments) {
            if (!argument.isValueSymbol()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Symbol cast(DataType newDataType, boolean tryCast) {
        if (newDataType instanceof ArrayType && info.ident().name().equals(ArrayFunction.NAME)) {
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
        DataType<?> innerType = ((ArrayType) newDataType).innerType();
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
        if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
            Symbols.nullableToStream(filter, out);
        }
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
        var sb = new StringBuilder(name + '(' + ExplainLeaf.printList(arguments) + ')');
        if (filter != null) {
            sb.append("FILTER (WHERE " + filter.representation() + ')');
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Function function = (Function) o;
        return Objects.equals(arguments, function.arguments) &&
               Objects.equals(info, function.info) &&
               Objects.equals(filter, function.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(arguments, info, filter);
    }
}
