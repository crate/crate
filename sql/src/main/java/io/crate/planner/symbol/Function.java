package io.crate.planner.symbol;

import io.crate.metadata.FunctionInfo;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class Function extends ValueSymbol {

    public static final SymbolFactory<Function> FACTORY = new SymbolFactory<Function>() {
        @Override
        public Function newInstance() {
            return new Function();
        }
    };

    private FunctionInfo info;

    public Function(FunctionInfo info) {
        this.info = info;
    }

    public Function() {

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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
    }
}
