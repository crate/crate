package crate.elasticsearch.action.import_;

import java.io.File;
import java.util.regex.Pattern;

public class ImportContext {

    private String nodePath;
    private boolean compression;
    private String directory;
    private Pattern file_pattern;
    private boolean mappings = false;
    private boolean settings = false;

    public ImportContext(String nodePath) {
        this.nodePath = nodePath;
    }

    public boolean compression() {
        return compression;
    }

    public void compression(boolean compression) {
        this.compression = compression;
    }

    public String directory() {
        return directory;
    }

    public void directory(String directory) {
        File file = new File(directory);
        if (!file.isAbsolute() && nodePath != null) {
            file = new File(nodePath, directory);
            directory = file.getAbsolutePath();
        }
        this.directory = directory;
    }

    public Pattern file_pattern() {
        return file_pattern;
    }

    public void file_pattern(Pattern file_pattern) {
        this.file_pattern = file_pattern;
    }

    public boolean mappings() {
        return mappings;
    }

    public void mappings(boolean mappings) {
        this.mappings = mappings;
    }

    public boolean settings() {
        return settings;
    }

    public void settings(boolean settings) {
        this.settings = settings;
    }
}
