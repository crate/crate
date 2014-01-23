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
        EqOperator op = new EqOperator(EqOperator.generateInfo(DataType.INTEGER));

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(new IntegerLiteral(2), new IntegerLiteral(2)));
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(true));
    }

    @Test
    public void testNormalizeSymbolNeq() {
        EqOperator op = new EqOperator(EqOperator.generateInfo(DataType.INTEGER));

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(new IntegerLiteral(2), new IntegerLiteral(4)));
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(false));
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        EqOperator op = new EqOperator(EqOperator.generateInfo(DataType.INTEGER));
        Function f1 = new Function(
                new FunctionInfo(
                        new FunctionIdent("dummy_function", Arrays.asList(DataType.INTEGER)),
                        DataType.INTEGER
                ),
                Arrays.<Symbol>asList(new IntegerLiteral(2))
        );

        Function f2 = new Function(
                new FunctionInfo(
                        new FunctionIdent("dummy_function", Arrays.asList(DataType.INTEGER)),
                        DataType.INTEGER
                ),
                Arrays.<Symbol>asList(new IntegerLiteral(2))
        );

        assertThat(f1.equals(f2), is(true)); // symbols are equal

        // EqOperator doesn't know (yet) if the result of the functions will be equal so no normalization
        Function function = new Function(op.info(), Arrays.<Symbol>asList(f1, f2));
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(Function.class));
    }
}
