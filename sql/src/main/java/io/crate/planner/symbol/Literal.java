package io.crate.planner.symbol;

import io.crate.operator.Input;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class Literal<T> extends ValueSymbol implements Input {

    public static Literal forType(DataType type, Object value) {
        switch (type) {
            case BYTE:
                break;
            case SHORT:
                break;
            case INTEGER:
                return new IntegerLiteral((Integer)value);
            case TIMESTAMP:
            case LONG:
                break;
            case FLOAT:
            case DOUBLE:
                return new DoubleLiteral((Number)value);
            case BOOLEAN:
                return new BooleanLiteral((Boolean)value);
            case IP:
            case STRING:
                return new StringLiteral((String)value);
            case OBJECT:
                break;
            case NOT_SUPPORTED:
                break;
        }

        return null;
    }
}
