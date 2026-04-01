package io.crate.execution.engine.aggregation.impl;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.NumericType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(RandomizedRunner.class)
public class NumericMinAggregationTest extends AggregationTestCase {

    private final String displayName;
    private final NumericType type;
    private final Object[][] data;
    private final BigDecimal expected;

    public NumericMinAggregationTest(String displayName, NumericType type, Object[][] data, BigDecimal expected) {
        this.displayName = displayName;
        this.type = type;
        this.data = data;
        this.expected = expected;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(
            new Object[]{
                "compact precision, simple case",
                new NumericType(6, 4),
                new Object[][]{{new BigDecimal("97.6543")}, {new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            },
            new Object[]{
                "large precision, simple case",
                new NumericType(22, 4),
                new Object[][]{{new BigDecimal("97.6543")}, {new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            },
            // See: https://github.com/crate/crate/pull/17371
            new Object[]{
                "compact precision, string sorting different",
                new NumericType(6, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("9.00")}},
                new BigDecimal("9.00")
            },
            // See: https://github.com/crate/crate/pull/17371
            new Object[]{
                "large precision, string sorting different",
                new NumericType(22, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("9.00")}},
                new BigDecimal("9.00")
            }
        );
    }

    @Test
    public void testNumeric() throws Exception {
        Object result = executeAggregation(type, data);
        assertThat(result).isEqualTo(expected);
    }

    protected Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.builder(MinimumAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(argumentType.getTypeSignature())
                .returnType(argumentType.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            data,
            List.of()
        );
    }
}
