package io.crate.types;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.test.integration.CrateUnitTest;

public class RowTypeTest extends CrateUnitTest {

    @Test
    public void test_row_type_streaming_roundtrip() throws Exception {
        var rowType = new RowType(List.of(DataTypes.STRING, DataTypes.INTEGER));

        var out = new BytesStreamOutput();
        rowType.writeTo(out);

        var in = out.bytes().streamInput();
        var streamedRowType = new RowType(in);

        assertThat(streamedRowType, Matchers.is(rowType));
    }

    @Test
    public void test_row_value_streaming_roundtrip() throws Exception {
        var row = new RowN(10, "foo", 23.4);
        var rowType = new RowType(List.of(DataTypes.INTEGER, DataTypes.STRING, DataTypes.DOUBLE));

        var out = new BytesStreamOutput();
        rowType.streamer().writeValueTo(out, row);

        var in = out.bytes().streamInput();
        var streamedRow = rowType.streamer().readValueFrom(in);

        assertThat(streamedRow, Matchers.is(row));
    }
}
