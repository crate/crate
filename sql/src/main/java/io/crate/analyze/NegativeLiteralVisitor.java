package io.crate.analyze;

import io.crate.planner.symbol.*;

public class NegativeLiteralVisitor extends SymbolVisitor<Void, Literal> {

    @Override
    public Literal visitDoubleLiteral(DoubleLiteral symbol, Void context) {
        return new DoubleLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteral symbol, Void context) {
        return new IntegerLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitBooleanLiteral(BooleanLiteral symbol, Void context) {
        return new BooleanLiteral(!symbol.value());
    }

    @Override
    public Literal visitLongLiteral(LongLiteral symbol, Void context) {
        return new LongLiteral(symbol.value() * -1);
    }

    @Override
    public Literal visitFloatLiteral(FloatLiteral symbol, Void context) {
        return new FloatLiteral(symbol.value() * -1);
    }

    @Override
    protected Literal visitSymbol(Symbol symbol, Void context) {
        throw new UnsupportedOperationException(String.format("Cannot negate symbol %s", symbol));
    }
}
