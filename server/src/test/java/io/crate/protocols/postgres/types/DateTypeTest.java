package io.crate.protocols.postgres.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.time.format.DateTimeParseException;

import static io.crate.testing.Asserts.assertThrows;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;

public class DateTypeTest extends BasePGTypeTest<Long> {

    public DateTypeTest() {
        super(DateType.INSTANCE);
    }

    @Test
    public void testBinaryRoundtrip() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            Long value = 1467072000000L;
            int written = pgType.writeAsBinary(buffer, value);
            int length = buffer.readInt();
            assertThat(written - 4, is(length));
            Long readValue = (Long) pgType.readBinaryValue(buffer, length);
            assertThat(readValue, is(value));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testEncodeAsUTF8Text() {
        assertThat(new String(DateType.INSTANCE.encodeAsUTF8Text(1467072000000L), UTF_8),
            is("2016-06-28"));
        assertThat(new String(DateType.INSTANCE.encodeAsUTF8Text(-93661920000000L), UTF_8),
            is("1000-12-22"));
    }

    @Test
    public void testDecodeUTF8TextWithUnexpectedFormat() {
        assertThrows(() -> DateType.INSTANCE.decodeUTF8Text("2016.06.28".getBytes(UTF_8)),
            DateTimeParseException.class, "");
    }

    @Test
    public void testDecodeUTF8Text() {
        assertThat(DateType.INSTANCE.decodeUTF8Text("2020-02-09".getBytes(UTF_8)),
            is(1581206400000L));
    }

}
