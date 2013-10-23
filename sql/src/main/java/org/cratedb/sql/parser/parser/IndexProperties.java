package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

import java.util.HashMap;
import java.util.Map;

public class IndexProperties extends QueryTreeNode {
    public static final String ANALYZER_PROPERTY = "analyzer";

    private final Map<String, ValueNode> keyValues = new HashMap<>();

    public void init() {}

    public void put(String key, ValueNode value) {
        keyValues.put(key, value);
    }

    public ValueNode get(String key) {
        return keyValues.get(key);
    }

    public String getAnalyzer() throws StandardException {
        ValueNode analyzerProperty = get(ANALYZER_PROPERTY);
        if (analyzerProperty == null) { return null; }
        return (String)analyzerProperty.getConstantValueAsObject();
    }

    public Iterable<Map.Entry<String, ValueNode>> iterator() {
        return keyValues.entrySet();
    }

    @Override
    public String toString() {
        return keyValues.toString();
    }
}
