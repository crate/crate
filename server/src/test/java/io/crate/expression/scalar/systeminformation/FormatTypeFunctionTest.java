package io.crate.expression.scalar.systeminformation;

import org.junit.Test;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;

public class FormatTypeFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void test_format_type_null_oid_returns_null() throws Exception {
        assertEvaluate("format_type(null, null)", null);
    }

    @Test
    public void test_format_type_for_unknown_oid_returns_questionmarks() throws Exception {
        assertEvaluate("format_type(2, null)", "???");
    }

    @Test
    public void test_format_type_for_known_oid_returns_type_name() throws Exception {
        assertEvaluate("format_type(25, null)", "text");
    }
}
