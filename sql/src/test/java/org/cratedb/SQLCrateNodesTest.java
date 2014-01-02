package org.cratedb;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.types.*;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.client.Client;

public class SQLCrateNodesTest extends CrateIntegrationTest {

    public static final ImmutableMap<DataType, SQLType> SQL_TYPES = new ImmutableMap.Builder<DataType, SQLType>()
        .put(DataType.BOOLEAN, new BooleanSQLType())
        .put(DataType.BYTE, new ByteSQLType())
        .put(DataType.SHORT, new ShortSQLType())
        .put(DataType.INTEGER, new IntegerSQLType())
        .put(DataType.LONG, new LongSQLType())
        .put(DataType.FLOAT, new FloatSQLType())
        .put(DataType.DOUBLE, new DoubleSQLType())
        .put(DataType.STRING, new StringSQLType())
        .put(DataType.OBJECT, new ObjectSQLType())
        .put(DataType.TIMESTAMP, new TimeStampSQLType())
        .put(DataType.IP, new IpSQLType())
        .build();

    public SQLResponse execute(Client client, String stmt, Object[]  args) {
        return client.execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public SQLResponse execute(Client client, String stmt) {
        return execute(client, stmt, new Object[0]);
    }

    public SQLResponse execute(String stmt, Object[] args) {
        return execute(client(), stmt, args);
    }

    public SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }
}
