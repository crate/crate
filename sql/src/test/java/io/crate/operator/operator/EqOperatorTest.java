package io.crate.operator.operator;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.TimestampLiteral;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class EqOperatorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testNormalizeSymbol() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.INTEGER));

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(new IntegerLiteral(2), new IntegerLiteral(2)));
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(true));
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.INTEGER));
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(Null.INSTANCE, Null.INSTANCE));
        Symbol result = op.normalizeSymbol(function);
        assertThat(result, instanceOf(Null.class));
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.INTEGER));
        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(new IntegerLiteral(2), Null.INSTANCE));
        Symbol result = op.normalizeSymbol(function);
        assertThat(result, instanceOf(Null.class));
    }

    @Test
    public void testNormalizeSymbolNeq() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.INTEGER));

        Function function = new Function(
                op.info(), Arrays.<Symbol>asList(new IntegerLiteral(2), new IntegerLiteral(4)));
        Symbol result = op.normalizeSymbol(function);

        assertThat(result, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral) result).value(), is(false));
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.INTEGER));
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

    @Test
    public void testEvaluateIntegerTrue() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.INTEGER));
        Boolean result = op.evaluate(
                new IntegerLiteral(1),
                new IntegerLiteral(1)
        );
        assertTrue(result);
    }

    @Test
    public void testEvaluateLongFalse() {
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.LONG));
        Boolean result = op.evaluate(
                new LongLiteral(1),
                new LongLiteral(2)
        );
        assertFalse(result);
    }

    @Test
    public void testEvaluateTimestampTrue() {
        String timestamp = "2014-02-19 13:38:00.1";
        TimestampLiteral ts = new TimestampLiteral(timestamp);
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.TIMESTAMP));
        Boolean result = op.evaluate(
                new LongLiteral(ts.getUnixTime()),
                new LongLiteral(ts.getUnixTime())
        );
        assertTrue(result);
    }

    @Test
    public void testEvaluateObjectTrue() {
        ObjectLiteral left = new ObjectLiteral(ImmutableMap.<String, Object>builder()
                .put("int", 1)
                .put("boolean", true)
                .build());
        ObjectLiteral right = new ObjectLiteral(ImmutableMap.<String, Object>builder()
                .put("int", 1)
                .put("boolean", true)
                .build());
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.OBJECT));
        Boolean result = op.evaluate(left, right);
        assertTrue(result);
    }

    @Test
    public void testEvaluateObjectFalse() {
        ObjectLiteral left = new ObjectLiteral(ImmutableMap.<String, Object>builder()
                .put("int", 1)
                .put("boolean", true)
                .build());
        ObjectLiteral right = new ObjectLiteral(ImmutableMap.<String, Object>builder()
                .put("int", 2)
                .put("boolean", false)
                .build());
        EqOperator op = new EqOperator(Operator.generateInfo(EqOperator.NAME, DataType.OBJECT));
        Boolean result = op.evaluate(left, right);
        assertFalse(result);
    }

    @Test
    public void testLookupComparator() {
        assertThat(CmpOperator.lookupComparator((byte) 1), instanceOf(CmpOperator.PrimitiveCmp.class));
        assertThat(CmpOperator.lookupComparator((short) 1), instanceOf(CmpOperator.PrimitiveCmp.class));
        assertThat(CmpOperator.lookupComparator(1), instanceOf(CmpOperator.PrimitiveCmp.class)); // int
        assertThat(CmpOperator.lookupComparator((long) 1), instanceOf(CmpOperator.PrimitiveCmp.class));
        assertThat(CmpOperator.lookupComparator((float) 1), instanceOf(CmpOperator.PrimitiveCmp.class));
        assertThat(CmpOperator.lookupComparator((double) 1), instanceOf(CmpOperator.PrimitiveCmp.class));
        assertThat(CmpOperator.lookupComparator(true), instanceOf(CmpOperator.PrimitiveCmp.class));
        assertThat(CmpOperator.lookupComparator("string"), instanceOf(CmpOperator.PrimitiveCmp.class));
        Map map = new HashMap<>();
        assertThat(CmpOperator.lookupComparator(map), instanceOf(CmpOperator.ObjectCmp.class));
    }

}
