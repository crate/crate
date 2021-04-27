package io.crate.expression.scalar;

import io.crate.metadata.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;


public class CurrentDateFunctionTest extends ScalarTestCase {

    private static final long CURRENT_TIMESTAMP = 1422294644581L;

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(CURRENT_TIMESTAMP);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void testCurdateReturnsSameValueAsDayTrunc() {
        assertEvaluate("CURDATE() = DATE_TRUNC('day', CURRENT_TIMESTAMP)", true);
    }

    @Test
    public void testCurdateReturnsExpectedDate() {
        var dayTruncMillis = LocalDate.ofInstant(Instant.ofEpochMilli(CURRENT_TIMESTAMP), ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEvaluate("CURDATE()", dayTruncMillis);
    }

    @Test
    public void testCurrentDateReturnsExpectedDate() {
        var dayTruncMillis = LocalDate.ofInstant(Instant.ofEpochMilli(CURRENT_TIMESTAMP), ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEvaluate("CURRENT_DATE", dayTruncMillis);
    }

    @Test
    public void testCurdateCallsWithinStatementAreIdempotent() {
        assertEvaluate("CURDATE() = CURDATE()", true);
    }

    @Test
    public void testCurrentDateCallsWithinStatementAreIdempotent() {
        assertEvaluate("CURRENT_DATE = CURRENT_DATE", true);
    }

}
