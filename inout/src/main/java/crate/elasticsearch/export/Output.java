package crate.elasticsearch.export;

import java.io.OutputStream;

public abstract class Output {

    private Result result;

    public class Result {
        public int exit;
        public String stdErr;
        public String stdOut;
    }

    public abstract void open() throws java.io.IOException;

    public abstract void close() throws java.io.IOException;

    public abstract OutputStream getOutputStream();

    public Result result() {
        return result;
    }
}
