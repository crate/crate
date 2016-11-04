package io.crate.rest.action.admin;


import io.crate.plugin.AdminUIPlugin;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.network.NetworkModule.HTTP_ENABLED;
import static org.hamcrest.core.Is.is;


public abstract class AdminUIHttpIntegrationTest extends ESIntegTestCase {

    protected InetSocketAddress address;
    protected CloseableHttpClient httpClient = HttpClients.createDefault();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HTTP_ENABLED, true)
            .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AdminUIPlugin.class);
    }


    @Before
    public void setup() throws ExecutionException, InterruptedException, IOException {
        Iterable<HttpServerTransport> transports = internalCluster().getInstances(HttpServerTransport.class);
        Iterator<HttpServerTransport> httpTransports = transports.iterator();
        address = ((InetSocketTransportAddress) httpTransports.next().boundAddress().publishAddress()).address();
        // place index file
        final Path indexDirectory = internalCluster().getInstance(Environment.class).pluginsFile().resolve("crate-admin").resolve("_site");
        Files.createDirectories(indexDirectory);
        final Path indexFile = indexDirectory.resolve("index.html");
        Files.write(indexFile, Arrays.asList("<h1>Crate Admin</h1>"), Charset.forName("UTF-8"));
    }

    protected CloseableHttpResponse executeAndDefaultAssertions(HttpUriRequest request) throws IOException {
        CloseableHttpResponse resp = httpClient.execute(request);
        assertThat(resp.containsHeader("Connection"), is(false));
        return resp;
    }

    protected CloseableHttpResponse get(String uri) throws IOException {
        return get(uri, null);
    }

    protected CloseableHttpResponse get(String uri, Header[] headers) throws IOException {
        HttpGet httpGet = new HttpGet(String.format(Locale.ENGLISH, "http://%s:%s/%s", address.getHostName(), address.getPort(), uri));
        if (headers != null) {
            httpGet.setHeaders(headers);
        }
        return executeAndDefaultAssertions(httpGet);
    }

    protected CloseableHttpResponse post(String uri) throws IOException {
        HttpPost httpPost = new HttpPost(String.format(Locale.ENGLISH, "http://%s:%s/%s", address.getHostName(), address.getPort(), uri));
        return executeAndDefaultAssertions(httpPost);
    }

    protected List<URI> getAllRedirectLocations(String link, Header[] headers) throws IOException {
        List<URI> redirectLocations = null;
        CloseableHttpResponse response = null;
        try {
            HttpClientContext context = HttpClientContext.create();
            HttpGet httpGet = new HttpGet(String.format(Locale.ENGLISH, "http://%s:%s/%s", address.getHostName(), address.getPort(), link));
            if (headers != null) {
                httpGet.setHeaders(headers);
            }
            response = httpClient.execute(httpGet, context);

            // get all redirection locations
            redirectLocations = context.getRedirectLocations();
        } finally {
            if(response != null) {
                response.close();
            }
        }

        return redirectLocations;
    }
}
