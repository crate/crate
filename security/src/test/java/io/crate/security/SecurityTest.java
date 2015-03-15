package io.crate.security;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.is;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.client.CrateClient;
import io.crate.security.util.ConfigConstants;
import io.crate.security.util.SecurityUtil;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataType;
import io.crate.types.StringType;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.SettableFuture;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST, numNodes = 3)
public class SecurityTest extends CrateIntegrationTest{

	static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }
	
    private CrateClient client;
	
	@Override
	protected Settings nodeSettings(int nodeOrdinal) {
		Settings localSettings = settingsBuilder()
                .put(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_ENABLED, true)
                .put(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_FILEPATH,SecurityUtil.getAbsoluteFilePathFromClassPath("node-"+nodeOrdinal+"-keystore.jks"))
                .put(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_FILEPATH,SecurityUtil.getAbsoluteFilePathFromClassPath("truststore.jks"))
                .put(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_ENCFORCE_HOSTNAME_VERIFICATION, false)
                .put(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_TRUSTSTORE_PASSWORD, "tspass")
                .put(ConfigConstants.SECURITY_SSL_TRANSPORT_NODE_KEYSTORE_PASSWORD, "kspass")
                .build();
		return localSettings;
	}

    @Before
    public void prepare() {
        int port = ((InetSocketTransportAddress) cluster()
                .getInstance(TransportService.class)
                .boundAddress().boundAddress()).address().getPort();
        
        Settings localSettings = settingsBuilder().
        		put(nodeSettings(0))
                .build();
        client = new CrateClient(localSettings, true).addTransportAddress(new InetSocketTransportAddress("localhost", port));
       
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
        super.tearDown();
    }

    @Test
    public void testCreateClient() throws Exception {
        client().prepareIndex("test", "default", "1")
            .setRefresh(true)
            .setSource("{}")
            .execute()
            .actionGet();
        ensureGreen();

        SQLResponse r = client.sql("select \"_id\" from test").actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("_id", r.cols()[0]);
        assertEquals("1", r.rows()[0][0]);

        assertThat(r.columnTypes(), is(new DataType[0]));
    }

    @Test
    public void testAsyncRequest() throws Throwable {
        client().prepareIndex("test", "default", "1")
                .setRefresh(true)
                .setSource("{}")
                .execute()
                .actionGet();
        ensureGreen();

        // In practice use ActionListener onResponse and onFailure to create a Promise instead
        final SettableFuture<Boolean> future = SettableFuture.create();
        final AtomicReference<Throwable> assertionError = new AtomicReference<>();

        ActionListener<SQLResponse> listener = new ActionListener<SQLResponse>() {
            @Override
            public void onResponse(SQLResponse r) {
                try {
                    assertEquals(1, r.rows().length);
                    assertEquals("_id", r.cols()[0]);
                    assertEquals("1", r.rows()[0][0]);

                    assertThat(r.columnTypes(), is(new DataType[0]));
                } catch (AssertionError e) {
                    assertionError.set(e);
                } finally {
                    future.set(true);
                }

            }
            @Override
            public void onFailure(Throwable e) {
                future.set(true);
                assertionError.set(e);
            }
        };
        client.sql("select \"_id\" from test", listener);

        // this will block until timeout is thrown if listener is not called
        assertThat(future.get(5L, TimeUnit.SECONDS), is(true));
        Throwable error = assertionError.get();
        if (error != null) {
            throw error;
        }
    }

    @Test
    public void testRequestWithTypes() throws Exception {
        client().prepareIndex("test", "default", "1")
            .setRefresh(true)
            .setSource("{}")
            .execute()
            .actionGet();
        ensureGreen();

        SQLRequest request =  new SQLRequest("select \"_id\" from test");
        request.includeTypesOnResponse(true);
        SQLResponse r = client.sql(request).actionGet();

        assertEquals(1, r.rows().length);
        assertEquals("_id", r.cols()[0]);
        assertEquals("1", r.rows()[0][0]);

        assertThat(r.columnTypes()[0], instanceOf(StringType.class));
    }

    @Test
    public void testSetSerialization() throws Exception {
        SQLResponse r = client.sql("select constraint_name " +
                "from information_schema.table_constraints").actionGet();
        assertTrue(r.rows()[0][0] instanceof Object[]);
        assertThat(((Object[]) r.rows()[0][0])[0], instanceOf(String.class));
    }

    @Test
    public void testSettings() throws Exception {
        Settings settings = client.settings();

        assertEquals(false, settings.getAsBoolean("network.server", true));
        assertEquals(true, settings.getAsBoolean("node.client", false));
        assertEquals(true, settings.getAsBoolean("client.transport.ignore_cluster_name", false));
        assertThat(settings.get("node.name"), startsWith("crate-client-"));
    }

    @Test
    public void testBulkSql() throws Exception {
        client.sql("create table test (a string, b int) with (number_of_replicas=0)").actionGet();
        ensureGreen();
        client.sql("insert into test (a, b) values ('foo', 1)").actionGet();
        client.sql("refresh table test").actionGet();

        SQLBulkRequest bulkRequest = new SQLBulkRequest(
                "update test set a = ? where b = ?",
                new Object[][]{new Object[]{"bar", 1}, new Object[]{"baz", 1}});
        SQLBulkResponse bulkResponse = client.bulkSql(bulkRequest).actionGet();
        assertThat(bulkResponse.results().length, is(2));
        for (SQLBulkResponse.Result result : bulkResponse.results()) {
            assertThat(result.rowCount(), is(1L));
            assertThat(result.errorMessage(), is(nullValue()));
        }
    }

    @Test
    public void testClusterSettingsSerialization() throws Exception {
        SQLResponse r = client.sql("select settings from sys.cluster").actionGet();
        assertThat(r.rowCount(), is(1L));
        assertTrue(r.rows()[0][0] instanceof Map);
    }

}
