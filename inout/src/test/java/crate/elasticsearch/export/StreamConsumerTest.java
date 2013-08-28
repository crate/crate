package crate.elasticsearch.export;

import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for the @StreamConsumer class.
 */
public class StreamConsumerTest {

    @Test
    public void test() {
        // Prepare an input stream with more than 8 bytes.
        String tmp = "one\ntwo\nthree\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(tmp.getBytes());

        // Initialize a consumer. Remember the first 8 bytes.
        StreamConsumer consumer = new StreamConsumer(inputStream, 8);

        // Immediately the consumer does not get any output yet.
        String output = consumer.getBufferedOutput();
        assertEquals("", output);

        // Wait for the stream to finish.
        consumer.waitFor();

        // The output delivers the first 8 bytes of the stream.
        output = consumer.getBufferedOutput();
        assertEquals("one\ntwo\n", output);

        // The input stream has no bytes left. The rest of the output
        // is consumed.
        assertEquals(0, inputStream.available());
    }

}
