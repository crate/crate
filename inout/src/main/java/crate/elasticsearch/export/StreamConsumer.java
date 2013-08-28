package crate.elasticsearch.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A stream consumer to consume the output of an input stream.
 */
public class StreamConsumer {

    private final StreamConsumerImpl impl;
    private Thread thread;

    /**
     * Constructor.
     *
     * @param inputStream the input stream to consume
     * @param bufferSize the buffer size to keep (first x bytes)
     */
    public StreamConsumer(InputStream inputStream, int bufferSize) {
        impl = new StreamConsumerImpl(inputStream, bufferSize);
        thread = new Thread(impl);
        thread.start();
    }

    /**
     * Get the buffered output of the stream (first x bytes defined
     * in buffer size)
     * @return
     */
    public String getBufferedOutput() {
        return impl.getOutput();
    }

    /**
     * Wait for the stream to finish.
     */
    public void waitFor() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implementation class for thread.
     */
    private final class StreamConsumerImpl implements Runnable {

        private final int bufferSize;
        private final StringBuffer collectedOutput = new StringBuffer();
        private InputStream inputStream;

        private StreamConsumerImpl(InputStream inputStream, int bufferSize) {
            this.bufferSize = bufferSize;
            this.inputStream = inputStream;
        }

        public void run() {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                    inputStream));
            String line;
            try {
                do {
                    line = bufferedReader.readLine();
                    if (line != null && collectedOutput.length() < bufferSize) {
                        collectedOutput.append(line + "\n");
                    }
                } while (line != null);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private String getOutput() {
            return collectedOutput.toString();
        }
    }
}
