package io.crate.sql.tree;


public enum Privilege {
    DQL("DQL"),
    DML("DML"),
    DDL("DDL");

    private final String name;

    Privilege(String name){
        this.name = name;
    }

    public boolean equalsName(String otherName) {
        return name.equals(otherName);
    }

    public String toString(){
        return this.name;
    }
}
