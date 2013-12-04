package org.cratedb.stats;

import java.util.Map;

public interface StatsInfo {

    public Map<String, Object> fields();

    public Object getStat(String columnName);

}
