package io.crate.analyze;

import java.util.ArrayList;
import java.util.List;

public class SubscriptContext {

    private String column;
    private List<String> parts = new ArrayList<>();

    public String column() {
        return column;
    }

    public void column(String column) {
        this.column = column;
    }

    public List<String> parts() {
        return parts;
    }

    public void add(String part) {
        parts.add(0, part);
    }
}
