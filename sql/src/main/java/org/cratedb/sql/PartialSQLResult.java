package org.cratedb.sql;


/**
 * A partial sql result, which does not contain any meta data.
 */
public interface PartialSQLResult {

    /**
     * Returns the the rows of the result.
     */
    public Object[][] rows();


    /**
     * Returns the number of rows processed.
     */
    public long rowCount();

}
