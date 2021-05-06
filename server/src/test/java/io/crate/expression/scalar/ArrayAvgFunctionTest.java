package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.crate.testing.Asserts.assertThrows;

public class ArrayAvgFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_avg_on_long_array_returns_numeric() {
        assertEvaluate("array_avg(long_array)",
            new BigDecimal(Long.MAX_VALUE),
            Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE), new ArrayType<>(DataTypes.LONG))
        );
    }

    @Test
    public void test_array_avg_on_short_array_returns_numeric() {
        assertEvaluate("array_avg(short_array)",
            new BigDecimal(Short.MAX_VALUE),
            Literal.of(List.of(Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE), new ArrayType<>(DataTypes.SHORT))
        );
    }

    @Test
    public void test_array_avg_on_byte_array_returns_numeric() {
        assertEvaluate("array_avg(?)",
            new BigDecimal(Byte.MAX_VALUE),
            Literal.of(List.of(Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE), new ArrayType<>(DataTypes.BYTE))
        );
    }

    @Test
    public void test_array_avg_on_int_array_returns_numeric() {
        assertEvaluate("array_avg(?)",
            new BigDecimal(Integer.MAX_VALUE),
            Literal.of(List.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE), new ArrayType<>(DataTypes.INTEGER))
        );
    }

    @Test
    public void test_array_avg_on_float_array_returns_float() {
        assertEvaluate("array_avg([1.0, 2.0] :: real[])",
            1.5f);
    }

    @Test
    public void test_array_avg_on_double_array_returns_double() {
        assertEvaluate("array_avg([1.0, 2.0] :: double precision[])",
            1.5d);
    }

    @Test
    public void test_array_avg_on_numeric_array_returns_numeric() {
        assertEvaluate("array_avg(?)",
            new BigDecimal(5.5d),
            Literal.of(List.of(BigDecimal.ONE, BigDecimal.TEN), new ArrayType<>(DataTypes.NUMERIC))
        );
    }

    @Test
    public void test_array_avg_ignores_null_element_values() {
        assertEvaluate("array_avg([null, 1])", BigDecimal.ONE);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluate("array_avg([null, null]::integer[])", null);
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluate("array_avg(null::int[])", null);
    }

    @Test
    public void test_array_avg_returns_null_for_null_values() {
        assertEvaluate("array_avg(null)", null);
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluate("array_avg(cast([] as array(integer)))", null);
    }

    @Test
    public void test_array_avg_with_array_of_undefined_inner_type_throws_exception() {
        assertThrows(() -> assertEvaluate("array_avg([])", null),
            UnsupportedOperationException.class,
            "Unknown function: array_avg([]), no overload found for matching argument types: (undefined_array).");
    }
}
