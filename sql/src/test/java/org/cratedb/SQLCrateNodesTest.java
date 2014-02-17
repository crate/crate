/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
