package io.crate.expression.scalar;

import static io.crate.testing.Asserts.assertThrowsMatches;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.junit.Test;

import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ArraySumFunctionTest extends ScalarTestCase {

    private final SessionSettings sessionSettings = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_array_returns_sum_of_elements() {

        // This test picks up random numbers but controls that overflow will not happen (overflow case is checked in another test).

        List<DataType> typesToTest = new ArrayList(DataTypes.NUMERIC_PRIMITIVE_TYPES);
        typesToTest.add(DataTypes.NUMERIC);

        for(DataType type: typesToTest) {
            var valuesToTest = TestingHelpers.getRandomsOfType(1, 10, type);

            DataType inputDependantOutputType = DataTypes.LONG;
            if (type == DataTypes.FLOAT || type == DataTypes.DOUBLE || type == DataTypes.NUMERIC) {
                inputDependantOutputType = type;
            } else {
                // check potential overflow and get rid of numbers causing overflow
                long sum = 0;
                for (int i = 0; i < valuesToTest.size(); i++) {
                    if (valuesToTest.get(i) != null) {
                        long nextNum = ((Number) valuesToTest.get(i)).longValue();
                        try {
                            sum = Math.addExact(sum, nextNum);
                        } catch (ArithmeticException e) {
                            valuesToTest = valuesToTest.subList(0, i); // excluding i
                            break;
                        }
                    }
                }
            }
            KahanSummationForDouble kahanSummationForDouble = new KahanSummationForDouble();
            var optional = valuesToTest.stream()
                .filter(Objects::nonNull)
                .reduce((o1, o2) -> {
                    if(o1 instanceof BigDecimal) {
                        return ((BigDecimal) o1).add((BigDecimal) o2);
                    } else if(o1 instanceof Double || o1 instanceof Float) {
                        return kahanSummationForDouble.sum(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
                    } else {
                        return DataTypes.LONG.implicitCast(o1, sessionSettings) + DataTypes.LONG.implicitCast(o2, sessionSettings);
                    }
                });
            var expected= inputDependantOutputType.implicitCast(optional.orElse(null), sessionSettings);


            String expression = String.format(Locale.ENGLISH,"array_sum(?::%s[])", type.getName());
            assertEvaluate(expression, expected, Literal.of(valuesToTest, new ArrayType<>(type)));
        }
    }

    @Test
    public void test_array_big_numbers_no_casting_results_in_exception() {
        assertThrowsMatches(() -> assertEvaluate("array_sum(?)", null,
                                Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE), new ArrayType<>(DataTypes.LONG))
                            ),
            ArithmeticException.class,
            "long overflow");
    }

    @Test
    public void test_array_big_numbers_casting_to_numeric_returns_sum() {
        assertEvaluate("array_sum(cast(long_array as array(numeric)))",
            new BigDecimal("18446744073709551614"),
            Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE), new ArrayType<>(DataTypes.LONG))
        );
    }

    @Test
    public void test_array_first_element_null_returns_sum() {
        assertEvaluate("array_sum([null, 1])", 1L);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluate("array_sum(cast([null, null] as array(integer)))", null);
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluate("array_sum(null::int[])", null);
    }

    @Test
    public void test_null_array_given_directly_results_in_null() {
        assertEvaluate("array_sum(null)", null);
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluate("array_sum(cast([] as array(integer)))", null);
    }

    @Test
    public void test_empty_array_given_directly_throws_exception() {
        assertThrowsMatches(() -> assertEvaluate("array_sum([])", null),
            UnsupportedOperationException.class,
            "Unknown function: array_sum([]), no overload found for matching argument types: (undefined_array).");
    }
}
