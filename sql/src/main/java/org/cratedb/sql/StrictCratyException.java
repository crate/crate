package org.cratedb.sql;

/**
 * Thrown on onvalid write operation an a strict craty type
 */
public class StrictCratyException extends CrateException {
    private final String columnName;

    public StrictCratyException(String columnName, Throwable e) {
        super("Invalid write operation on strict craty", e);
        this.columnName = columnName;
    }

    @Override
    public int errorCode() {
        return 4004;
    }

    @Override
    public Object[] args() {
        return new Object[]{columnName};
    }
}
