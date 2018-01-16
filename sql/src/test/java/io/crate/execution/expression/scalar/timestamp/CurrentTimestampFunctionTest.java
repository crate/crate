package io.crate.execution.expression.scalar.timestamp;

import io.crate.analyze.symbol.Literal;
import io.crate.execution.expression.scalar.AbstractScalarFunctionsTest;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;

public class CurrentTimestampFunctionTest extends AbstractScalarFunctionsTest {

    private static final long EXPECTED_TIMESTAMP = 1422294644581L;

    @Before
    public void prepare() {
        DateTimeUtils.setCurrentMillisFixed(EXPECTED_TIMESTAMP);
    }

    @After
    public void cleanUp() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void timestampIsCreatedCorrectly() {
        assertEvaluate("current_timestamp", EXPECTED_TIMESTAMP);
    }

    @Test
    public void precisionOfZeroDropsAllFractionsOfSeconds() {
        assertEvaluate("current_timestamp(0)", EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 1000));
    }

    @Test
    public void precisionOfOneDropsLastTwoDigitsOfFractionsOfSecond() {
        assertEvaluate("current_timestamp(1)", EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 100));
    }

    @Test
    public void precisionOfTwoDropsLastDigitOfFractionsOfSecond() {
        assertEvaluate("current_timestamp(2)", EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 10));
    }

    @Test
    public void precisionOfThreeKeepsAllFractionsOfSeconds() {
        assertEvaluate("current_timestamp(3)", EXPECTED_TIMESTAMP);
    }

    @Test
    public void precisionLargerThan3RaisesException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Precision must be between 0 and 3");
        assertEvaluate("current_timestamp(4)", null);
    }

    @Test
    public void integerIsNormalizedToLiteral() {
        assertNormalize("current_timestamp(1)", instanceOf(Literal.class));
    }
}
