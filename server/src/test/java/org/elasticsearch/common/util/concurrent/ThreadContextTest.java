package org.elasticsearch.common.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.jupiter.api.Test;

public class ThreadContextTest {

    @Test
    public void test_serialization_bwc() throws Exception {
        var out = new BytesStreamOutput();
        out.setVersion(Version.V_4_0_0);
        ThreadContext.bwcWriteHeaders(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_4_0_0);
        ThreadContext.bwcReadHeaders(in);
        assertThat(in.available(), is(0));
    }

    @Test
    public void test_serialization_current() throws Exception {
        var out = new BytesStreamOutput();
        ThreadContext.bwcWriteHeaders(out);

        var in = out.bytes().streamInput();
        ThreadContext.bwcReadHeaders(in);
        assertThat(in.available(), is(0));
    }
}
