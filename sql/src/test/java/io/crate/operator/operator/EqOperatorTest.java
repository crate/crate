package io.crate.operator.operator;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EqOperatorTest {


    @Test
    public void testNormalizeSymbol() {
        FunctionInfo info = new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.asList(DataType.INTEGER, DataType.INTEGER)),
                DataType.BOOLEAN
        );
        EqOperator op = new EqOperator(info);

        Function function = new Function(info, Arrays.<Symbol>asList(new IntegerLiteral(2), new IntegerLiteral(2)));
        Symbol result = op.normalizeSymbol(function);

        assertThat(
                result,
                instanceOf(BooleanLiteral.class)
        );

        assertThat(((BooleanLiteral) result).value(), is(true));
    }

    @Test
    public void testNormalizeSymbolNeq() {
        FunctionInfo info = new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.asList(DataType.INTEGER, DataType.INTEGER)),
                DataType.BOOLEAN
        );
        EqOperator op = new EqOperator(info);

        Function function = new Function(info, Arrays.<Symbol>asList(new IntegerLiteral(2), new IntegerLiteral(4)));
        Symbol result = op.normalizeSymbol(function);

        assertThat(
                result,
                instanceOf(BooleanLiteral.class)
        );

        assertThat(((BooleanLiteral) result).value(), is(false));
    }
}
