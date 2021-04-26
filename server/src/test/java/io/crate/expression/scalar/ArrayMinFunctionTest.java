package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.crate.testing.Asserts.assertThrows;

public class ArrayMinFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_returns_min_element() {
        List<DataType> typesToTest = new ArrayList(DataTypes.PRIMITIVE_TYPES);
        typesToTest.add(DataTypes.NUMERIC);

        for(DataType type: typesToTest) {
            var valuesToTest = TestingHelpers.getRandomsOfType(2, 10, type);

            var optional = valuesToTest.stream()
                .filter(o -> o != null)
                .min((o1, o2) -> type.compare(o1, o2));
            var expected = optional.orElse(null);

            String expression = String.format(Locale.ENGLISH, "array_min(?::%s[])", type.getName());
            assertEvaluate(expression, expected, Literal.of(valuesToTest, new ArrayType<>(type)));
        }
    }

    @Test
    public void test_array_first_element_null_returns_min() {
        assertEvaluate("array_min([null, 1])", 1);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluate("array_min(cast([null, null] as array(integer)))", null);
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluate("array_min(null::int[])", null);
    }

    @Test
    public void test_null_array_given_directly_results_in_null() {
        assertEvaluate("array_min(null)", null);
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluate("array_min(cast([] as array(integer)))", null);
    }

    @Test
    public void test_empty_array_given_directly_throws_exception() {
        assertThrows(() -> assertEvaluate("array_min([])", null),
            UnsupportedOperationException.class,
            "Unknown function: array_min([]), no overload found for matching argument types: (undefined_array).");
    }
}
