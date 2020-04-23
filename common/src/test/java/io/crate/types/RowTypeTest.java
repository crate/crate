package io.crate.types;

import io.crate.data.RowN;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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

    @Test
    public void test_row_type_create_type_from_signature_round_trip() {
        var expected = new RowType(
            List.of(DataTypes.INTEGER, DataTypes.STRING),
            List.of("field1", "field2"));

        var actual = expected.getTypeSignature().createType();

        assertThat(actual, instanceOf(RowType.class));
        assertThat(actual.getTypeParameters(), is(equalTo(expected.getTypeParameters())));
    }
}
