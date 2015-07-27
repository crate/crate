package io.crate.integrationtests;


import io.crate.action.sql.SQLResponse;
import org.junit.Test;

public class CrateSettingsIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testAllSettingsAreSelectable() throws Exception {
        SQLResponse res = execute("select schema_name, table_name, column_name from information_schema.columns where column_name like 'settings%'");
        for (Object[] row : res.rows()) {
            execute(String.format("select %s from %s.%s ", row[2], row[0], row[1]));
        }
    }
}
