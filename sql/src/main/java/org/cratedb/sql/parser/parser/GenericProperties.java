package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

import java.util.HashMap;
import java.util.Map;

public class GenericProperties extends QueryTreeNode {

    private final Map<String, QueryTreeNode> keyValues = new HashMap<>();

    public void init() {}

    @Override
    public void init(Object properties) throws StandardException {
        if (properties != null) {
            this.copyFrom((QueryTreeNode)properties);
        }

    }

    @Override
    public void copyFrom(QueryTreeNode other) throws StandardException {
        super.copyFrom(other);
        GenericProperties properties = (GenericProperties) other;
        this.keyValues.clear();
        if (properties.hasProperties()) {
            for (Map.Entry<String, QueryTreeNode> entry : properties.iterator()) {
                this.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void put(String key, QueryTreeNode value) {
        keyValues.put(key, value);
    }

    public QueryTreeNode get(String key) {
        return keyValues.get(key);
    }


    public Iterable<Map.Entry<String, QueryTreeNode>> iterator() {
        return keyValues.entrySet();
    }
    public boolean hasProperties() {
        return this.keyValues.size() > 0;
    }

    @Override
    public String toString() {
        return keyValues.toString();
    }
}
