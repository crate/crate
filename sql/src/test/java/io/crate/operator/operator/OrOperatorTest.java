package io.crate.operator.operator;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class OrOperatorTest {

    @Test
    public void testOptimizeSymbol() throws Exception {
        FunctionInfo info = new FunctionInfo(
                new FunctionIdent(OrOperator.NAME, Arrays.asList(DataType.BOOLEAN, DataType.BOOLEAN)),
                DataType.BOOLEAN
        );
        OrOperator operator = new OrOperator(info);

        Function function = new Function(info, Arrays.asList(new Reference(), new BooleanLiteral(true)));
        Symbol optimizedSymbol = operator.optimizeSymbol(function);
        assertThat(optimizedSymbol, instanceOf(BooleanLiteral.class));
    }

    @Test
    public void testOptimizeSymbolUnoptimizable() throws Exception {
        FunctionInfo info = new FunctionInfo(
                new FunctionIdent(OrOperator.NAME, Arrays.asList(DataType.BOOLEAN, DataType.BOOLEAN)),
                DataType.BOOLEAN
        );
        OrOperator operator = new OrOperator(info);

        Function function = new Function(info, Arrays.<ValueSymbol>asList(new Reference(), new Reference()));
        Symbol unoptimized = operator.optimizeSymbol(function);
        assertThat(unoptimized, instanceOf(Function.class));
    }
}