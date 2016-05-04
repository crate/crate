package io.crate.operation.scalar.timestamp;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CurrentTimestampFunctionTest extends AbstractScalarFunctionsTest {

    private CurrentTimestampFunction timestampFunction;
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final long EXPECTED_TIMESTAMP = 1422294644581L;

    @Before
    public void prepare() {
        DateTimeUtils.setCurrentMillisFixed(EXPECTED_TIMESTAMP);
        timestampFunction = new CurrentTimestampFunction();
    }

    @After
    public void cleanUp() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void timestampIsCreatedCorrectly() {
        String expectedTime = new SimpleDateFormat(DATE_FORMAT).format(new Date(EXPECTED_TIMESTAMP));
        String received = new SimpleDateFormat(DATE_FORMAT).format(new Date(timestampFunction.evaluate()));
        assertThat(received, is(expectedTime));
    }

    @Test
    public void precisionOfZeroDropsAllFractionsOfSeconds() {
        Long timestamp = timestampFunction.evaluate(Literal.newLiteral(0));
        assertThat(timestamp, is(EXPECTED_TIMESTAMP - EXPECTED_TIMESTAMP % 1000));
    }

    @Test
    public void precisionOfOneDropsLastTwoDigitsOfFractionsOfSecond() {
        Long timestamp = timestampFunction.evaluate(Literal.newLiteral(1));
        assertThat(timestamp, is(EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 100)));
    }

    @Test
    public void precisionOfTwoDropsLastDigitOfFractionsOfSecond() {
        Long timestamp = timestampFunction.evaluate(Literal.newLiteral(2));
        assertThat(timestamp, is(EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 10)));
    }

    @Test
    public void precisionOfThreeKeepsAllFractionsOfSeconds() {
        Long timestamp = timestampFunction.evaluate(Literal.newLiteral(2));
        assertThat(timestamp, is(EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 10)));
    }

    @Test
    public void precisionLessThanZeroRaisesException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Precision must be between 0 and 3");
        timestampFunction.evaluate(Literal.newLiteral(-1));
    }

    @Test
    public void precisionLargerThan3RaisesException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Precision must be between 0 and 3");
        timestampFunction.evaluate(Literal.newLiteral(4));
    }

    @Test
    public void precisionOfNullLRaisesException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("NULL precision not supported for current_timestamp");
        timestampFunction.evaluate(Literal.newLiteral((Integer) null));
    }

    @Test
    public void integerIsNormalizedToLiteral() {
        assertNormalize("current_timestamp(1)", instanceOf(Literal.class));
    }
}
