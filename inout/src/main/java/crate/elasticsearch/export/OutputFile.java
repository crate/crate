package crate.elasticsearch.export;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class OutputFile extends Output {

    private Result result;
    private final String path;
    private OutputStream os;
    private final boolean overwrite;
    private final boolean compression;

    public OutputFile(String path, boolean overwrite, boolean compression) {
        this.path = path;
        this.overwrite = overwrite;
        this.compression = compression;
    }

    @Override
    public void open() throws IOException {
        File outFile = new File(path);
        if (!overwrite && outFile.exists()){
            throw new IOException("File exists: " +  path);
        }
        os = new FileOutputStream(outFile);
        if (compression) {
            os = new GZIPOutputStream(os);
        }
    }

    @Override
    public void close() throws IOException {
        result = new Result();
        if (os != null) {
            os.close();
            result.exit = 0;
        } else {
            result.exit = 1;
        }
        os = null;
    }

    @Override
    public OutputStream getOutputStream() {
        return os;
    }

    @Override
    public Result result() {
        return result;
    }
}
