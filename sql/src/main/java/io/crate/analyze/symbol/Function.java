package io.crate.analyze.symbol;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.ExplainLeaf;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
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
