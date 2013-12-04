package org.cratedb.stats;

import org.cratedb.sql.TableUnknownException;

public class StatsTableUnknownException extends TableUnknownException {

    public StatsTableUnknownException(String tableName) {
        super(tableName);
    }
}
