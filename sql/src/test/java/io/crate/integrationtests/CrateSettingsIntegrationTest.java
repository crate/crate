package io.crate.integrationtests;


import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.is;

@UseJdbc
public class CrateSettingsIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testAllSettingsAreSelectable() throws Exception {
        SQLResponse res = execute("select table_schema, table_name, column_name from information_schema.columns where column_name like 'settings%'");
        for (Object[] row : res.rows()) {
            execute(String.format(Locale.ENGLISH, "select %s from %s.%s ", row[2], row[0], row[1]));
        }
    }

    @Test
    @Deprecated
    public void testSetDeprecatedSettingAsObjectLiteral() throws Exception {
        execute("set global indices = {\"fielddata\"={\"breaker\"={\"limit\"='2mb', \"overhead\"=2.0}}}");
        execute("select settings['indices']['breaker']['fielddata']['limit'] from sys.cluster");
        assertThat(response.rows()[0][0], is("2mb"));
        execute("select settings['indices']['breaker']['fielddata']['overhead'] from sys.cluster");
        assertThat(response.rows()[0][0], is(2.0));
        execute("reset global indices['breaker']['fielddata']");
    }

    @Test
    @Deprecated
    public void testApplyNewSettingIfDeprecated() {
        execute("set global indices['fielddata']['breaker']['limit'] = '10mb'");
        execute("set global indices['fielddata']['breaker']['overhead'] = 2.0");

        execute("select settings['indices']['breaker']['fielddata']['limit'] from sys.cluster");
        assertThat(response.rows()[0][0], is("10mb"));
        execute("select settings['indices']['breaker']['fielddata']['overhead'] from sys.cluster");
        assertThat(response.rows()[0][0], is(2.0));

        execute("reset global indices['breaker']['fielddata']");
    }
}
