package io.crate.rest.action.admin;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class AdminUIIntegrationTest extends AdminUIHttpIntegrationTest {

    private URI adminURI() throws URISyntaxException {
        return new URI(String.format(Locale.ENGLISH, "http://%s:%d/admin/", address.getHostName(), address.getPort()));
    }

    @Test
    public void testNonBrowserRequestToRoot() throws IOException {
        //request to root
        CloseableHttpResponse response = get("");

        //status should be 200 OK
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        //response body should not be null
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, notNullValue());

        //check content-type of response is json
        String contentMimeType = ContentType.getOrDefault(response.getEntity()).getMimeType();
        assertThat(contentMimeType, equalTo(ContentType.APPLICATION_JSON.getMimeType()));
    }

    @Test
    public void testBrowserRequestToRoot() throws IOException, URISyntaxException {
        Header[] headers = {
            new BasicHeader("uSer-AgEnt", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36")
        };

        List<URI> allRedirectLocations = getAllRedirectLocations("", headers);

        // allRedirectLocations should not be null
        assertThat(allRedirectLocations, notNullValue());
        // allRedirectLocations should contain the crateAdminUI URI
        assertThat(allRedirectLocations.contains(adminURI()), is(true));
    }

    @Test
    public void testBrowserRequestToAdmin() throws IOException, URISyntaxException {
        //request to admin
        Header[] headers = {
            new BasicHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36")
        };

        List<URI> allRedirectLocations = getAllRedirectLocations("admin", headers);

        // all redirect locations should not be null
        assertThat(allRedirectLocations, notNullValue());
        // all redirect locations should contain the crateAdminUI URI
        assertThat(allRedirectLocations.contains(adminURI()), is(true));
    }

    @Test
    public void testPostForbidden() throws IOException {
        CloseableHttpResponse response = post("/admin/");
        //status should be 403 FORBIDDEN
        assertThat(response.getStatusLine().getStatusCode(), is(403));
    }

    @Test
    public void testGetIndexHTML() throws IOException {
        assertIsIndexResponse(get("/admin/"));
        assertIsIndexResponse(get("/admin/index.html"));
        assertIsIndexResponse(get("/admin//index.html"));
    }

    @Test
    public void testTrailingSlash() throws Exception {
        // trailing slash is removed
        assertIsIndexResponse(get("/admin/index.html/"));
    }

    @Test
    public void testNotFound() throws Exception {
        CloseableHttpResponse response = get("/admin/does/not/exist.html");
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    private static void assertIsIndexResponse(CloseableHttpResponse response) throws IOException {
        //response body should not be null
        String bodyAsString = EntityUtils.toString(response.getEntity());
        assertThat(bodyAsString, is("<h1>Crate Admin</h1>\n"));
        assertThat(response.getHeaders("Content-Type")[0].getValue(), is("text/html"));
    }
}
