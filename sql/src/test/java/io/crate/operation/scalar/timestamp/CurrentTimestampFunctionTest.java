package io.crate.operation.scalar.timestamp;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static io.crate.testing.TestingHelpers.createFunction;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CurrentTimestampFunctionTest extends CrateUnitTest {

    private CurrentTimestampFunction timestampFunction;
    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    public static final long EXPECTED_TIMESTAMP = 1422294644581L;

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
        expectedException.expectMessage("NULL precision not supported for CURRENT_TIMESTAMP");
        timestampFunction.evaluate(Literal.newLiteral((Integer) null));
    }

    @Test
    public void integerIsNormalizedToLiteral() {
        Function function = createFunction("CURRENT_TIMESTAMP", DataTypes.TIMESTAMP, Arrays.<Symbol>asList(Literal.newLiteral(1)));
        assertThat(timestampFunction.normalizeSymbol(function), instanceOf(Literal.class));
    }

    @Test
    public void normalizeReferenceRaisesException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid argument to CURRENT_TIMESTAMP");
        Reference ref = TestingHelpers.createReference("Some_Column", DataTypes.INTEGER);
        Function function = createFunction("CURRENT_TIMESTAMP", DataTypes.TIMESTAMP, Arrays.<Symbol>asList(ref));
        timestampFunction.normalizeSymbol(function);
    }
}
