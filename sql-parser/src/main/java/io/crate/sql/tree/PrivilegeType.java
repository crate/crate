package io.crate.sql.tree;


public enum PrivilegeType {
    DQL("DQL"),
    DML("DML"),
    DDL("DDL"),
    DCL("DCL");

    private final String name;

    PrivilegeType(String name){
        this.name = name;
    }

    public boolean equalsName(String otherName) {
        return name.equals(otherName);
    }

    public String toString(){
        return this.name;
    }
}
