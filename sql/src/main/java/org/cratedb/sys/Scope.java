package org.cratedb.sys;

/**
 * Scopes in which system expressions can be evaluated.
 */
public enum Scope {

    CLUSTER("cluster"),
    NODE("node"),
    TABLE("table"),
    SHARD("shard"),
    DOC("doc");

    private final String name;

    private Scope(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
};
