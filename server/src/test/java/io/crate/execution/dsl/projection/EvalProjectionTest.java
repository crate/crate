
package io.crate.execution.dsl.projection;

import static org.hamcrest.CoreMatchers.is;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.metadata.RowGranularity;

public class EvalProjectionTest extends ESTestCase {

    @Test
    public void test_granularity_property_is_serialized() throws Exception {
        var projection = new EvalProjection(List.of(Literal.of(10)), RowGranularity.SHARD);
        var out = new BytesStreamOutput();
        projection.writeTo(out);

        var in = out.bytes().streamInput();
        var inProjection  = new EvalProjection(in);
        assertThat(projection.requiredGranularity(), is(inProjection.requiredGranularity()));
        assertThat(projection.outputs(), is(inProjection.outputs()));
    }
}
