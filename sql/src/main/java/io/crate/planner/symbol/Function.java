package io.crate.planner.symbol;

import io.crate.metadata.FunctionInfo;
import org.cratedb.DataType;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Function extends ValueSymbol {

    public static final SymbolFactory<Function> FACTORY = new SymbolFactory<Function>() {
        @Override
        public Function newInstance() {
            return new Function();
        }
    };

    private List<ValueSymbol> arguments;
    private FunctionInfo info;

    public Function(FunctionInfo info, List<ValueSymbol> arguments) {
        Preconditions.checkArgument(arguments.size() == info.ident().argumentTypes().size());
        this.info = info;
        this.arguments = arguments;
    }

    public Function() {

    }

    public List<ValueSymbol> arguments() {
        return arguments;
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
            arguments.add((ValueSymbol)Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
        out.writeVInt(arguments.size());
        for (ValueSymbol argument : arguments) {
            Symbol.toStream(argument, out);
        }
    }
}