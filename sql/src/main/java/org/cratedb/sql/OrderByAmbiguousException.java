package org.cratedb.sql;

public class OrderByAmbiguousException extends CrateException {

    private final String orderByName;

    public OrderByAmbiguousException(String orderByName) {
        super(String.format("ORDER BY \"%s\" is ambiguous", orderByName));
        this.orderByName = orderByName;
    }
}
