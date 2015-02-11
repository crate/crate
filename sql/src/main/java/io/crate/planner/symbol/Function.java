package io.crate.planner.symbol;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Function extends Symbol implements Cloneable {

    public static final SymbolFactory<Function> FACTORY = new SymbolFactory<Function>() {
        @Override
        public Function newInstance() {
            return new Function();
        }
    };

    private List<Symbol> arguments;
    private FunctionInfo info;

    public Function(FunctionInfo info, List<Symbol> arguments) {
        Preconditions.checkNotNull(info, "function info is null");
        Preconditions.checkArgument(arguments.size() == info.ident().argumentTypes().size());
        this.info = info;

        assert arguments.isEmpty() || !(arguments instanceof ImmutableList) :
                "must not be an immutable list - would break setArgument";
        this.arguments = arguments;
    }

    private Function() {

    }

    public List<Symbol> arguments() {
        return arguments;
    }

    public void setArgument(int index, Symbol symbol) {
        arguments.set(index, symbol);
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
    public void readFrom(StreamInput in) throws IOException {
        info = new FunctionInfo();
        info.readFrom(in);

        int numArguments = in.readVInt();
        arguments = new ArrayList<>(numArguments);
        for (int i = 0; i < numArguments; i++) {
            arguments.add(Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
        out.writeVInt(arguments.size());
        for (Symbol argument : arguments) {
            Symbol.toStream(argument, out);
        }
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", info.ident().name(), Joiner.on(",").join(arguments()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Function function = (Function) o;

        if (arguments != null ? !arguments.equals(function.arguments) : function.arguments != null)
            return false;
        if (info != null ? !info.equals(function.info) : function.info != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = arguments != null ? arguments.hashCode() : 0;
        result = 31 * result + (info != null ? info.hashCode() : 0);
        return result;
    }

    @Override
    public Function clone() {
        List<Symbol> args = new ArrayList<>(this.arguments);
        return new Function(this.info, args);
    }
}