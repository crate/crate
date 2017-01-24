package io.crate.integrationtests;


import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.*;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class BlobIntegrationTest extends BlobHttpIntegrationTest {

    private String uploadSmallBlob() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri(digest), StringUtils.repeat("a", 1500));
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        return digest;
    }

    private String uploadBigBlob() throws IOException {
        String digest = "37ca53ed215ea5e0e7fb67e5e12b4ff41dd5eeb0";
        put(blobUri(digest), StringUtils.repeat("abcdefghijklmnopqrstuvwxyz", 1024 * 600));
        return digest;
    }

    private String uploadTinyBlob() throws IOException {
        String digest = "32d10c7b8cf96570ca04ce37f2a19d84240d3a89";
        put(blobUri(digest), "abcdefghijklmnopqrstuvwxyz");
        return digest;
    }

    @Test
    public void testUploadInvalidSha1() throws IOException {
        CloseableHttpResponse response = put("test/d937ea65641c23fadc83616309e5b0e11acc5806", "asdf");
        assertThat(response.getStatusLine().getStatusCode(), is(400));
    }

    @Test
    public void testNonExistingFile() throws IOException {
        CloseableHttpResponse response = get("test/d937ea65641c23fadc83616309e5b0e11acc5806");
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testUploadValidFile() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri(digest), StringUtils.repeat("a", 1500));
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        /* Note that the content length is specified in the response in order to
        let keep alive clients know that they don't have to wait for data
        after the put and may close the connection if appropriate */
        assertThat(response.getFirstHeader("Content-Length").getValue(), is("0"));
    }

    @Test
    public void testUploadChunkedWithConflict() throws IOException {
        String digest = uploadBigBlob();
        CloseableHttpResponse conflictRes = put(blobUri(digest), StringUtils.repeat("abcdefghijklmnopqrstuvwxyz",
            1024 * 600));
        assertThat(conflictRes.getStatusLine().getStatusCode(), is(409));
    }

    @Test
    public void testUploadToUnknownBlobTable() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri("test_no_blobs", digest), StringUtils.repeat("a", 1500));
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testGetFiles() throws IOException {
        String digest = uploadBigBlob();
        CloseableHttpResponse res = get(blobUri(digest));
        assertThat(res.getEntity().getContentLength(), is(15974400L));
    }

    @Test
    public void testHeadRequest() throws IOException {
        String digest = uploadSmallBlob();
        CloseableHttpResponse res = head(blobUri(digest));
        assertThat(res.getFirstHeader("Content-Length").getValue(), is("1500"));
        assertThat(res.getFirstHeader("Accept-Ranges").getValue(), is("bytes"));
        assertThat(res.getFirstHeader("Expires").getValue(), is("Thu, 31 Dec 2037 23:59:59 GMT"));
        assertThat(res.getFirstHeader("Cache-Control").getValue(), is("max-age=315360000"));
    }

    @Test
    public void testRedirect() throws IOException {
        // One of the head requests must be redirected:
        String digest = uploadSmallBlob();

        int numberOfRedirects1 = getNumberOfRedirects(blobUri(digest), address);
        assertThat(numberOfRedirects1, greaterThanOrEqualTo(0));

        int numberOfRedirects2 = getNumberOfRedirects(blobUri(digest), address2);
        assertThat(numberOfRedirects2, greaterThanOrEqualTo(0));

        assertThat(numberOfRedirects1, is(not(numberOfRedirects2)));
    }

    @Test
    public void testDeleteFile() throws IOException {
        String digest = uploadSmallBlob();
        String uri = blobUri(digest);
        CloseableHttpResponse res = delete(uri);
        assertThat(res.getStatusLine().getStatusCode(), is(204));

        res = get(uri);
        assertThat(res.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testByteRange() throws IOException {
        String digest = uploadTinyBlob();
        Header[] headers = {
            new BasicHeader("Range", "bytes=8-")
        };
        CloseableHttpResponse res = get(blobUri(digest), headers);
        assertThat(res.getFirstHeader("Content-Length").getValue(), is("18"));
        assertThat(res.getFirstHeader("Content-Range").getValue(), is("bytes 8-25/26"));
        assertThat(res.getFirstHeader("Accept-Ranges").getValue(), is("bytes"));
        assertThat(res.getFirstHeader("Expires").getValue(), is("Thu, 31 Dec 2037 23:59:59 GMT"));
        assertThat(res.getFirstHeader("Cache-Control").getValue(), is("max-age=315360000"));
        assertThat(EntityUtils.toString(res.getEntity()), is("ijklmnopqrstuvwxyz"));

        res = get(blobUri(digest), new Header[]{
            new BasicHeader("Range", "bytes=0-1")
        });
        assertThat(EntityUtils.toString(res.getEntity()), is("ab"));

        res = get(blobUri(digest), new Header[]{
            new BasicHeader("Range", "bytes=25-")
        });
        assertThat(EntityUtils.toString(res.getEntity()), is("z"));
    }

    @Test
    public void testInvalidByteRange() throws IOException {
        String digest = uploadTinyBlob();
        Header[] headers = {
            new BasicHeader("Range", "bytes=40-58")
        };
        CloseableHttpResponse res = get(blobUri(digest), headers);
        assertThat(res.getStatusLine().getStatusCode(), is(416));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Requested Range Not Satisfiable"));
        assertThat(res.getFirstHeader("Content-Length").getValue(), is("0"));
    }

    @Test
    public void testParallelAccess() throws Throwable {
        String digest = uploadBigBlob();
        String expectedContent = StringUtils.repeat("abcdefghijklmnopqrstuvwxyz", 1024 * 600);
        Header[][] headers = new Header[40][];
        String[] uris = new String[40];
        String[] expected = new String[40];
        for (int i = 0; i < 40; i++) {
            headers[i] = new Header[]{};
            uris[i] = blobUri(digest);
            expected[i] = expectedContent;
        }
        assertThat(mget(uris, headers, expected), is(true));
    }

    @Test
    public void testParallelAccessWithRange() throws Throwable {
        String digest = uploadBigBlob();
        String expectedContent = StringUtils.repeat("abcdefghijklmnopqrstuvwxyz", 1024 * 600);
        Header[][] headers = new Header[][]{
            {new BasicHeader("Range", "bytes=0-")},
            {new BasicHeader("Range", "bytes=10-100")},
            {new BasicHeader("Range", "bytes=20-30")},
            {new BasicHeader("Range", "bytes=40-50")},
            {new BasicHeader("Range", "bytes=40-80")},
            {new BasicHeader("Range", "bytes=10-80")},
            {new BasicHeader("Range", "bytes=5-30")},
            {new BasicHeader("Range", "bytes=15-3000")},
            {new BasicHeader("Range", "bytes=2000-10800")},
            {new BasicHeader("Range", "bytes=1500-20000")},
        };
        String[] expected = new String[]{
            expectedContent,
            expectedContent.substring(10, 101),
            expectedContent.substring(20, 31),
            expectedContent.substring(40, 51),
            expectedContent.substring(40, 81),
            expectedContent.substring(10, 81),
            expectedContent.substring(5, 31),
            expectedContent.substring(15, 3001),
            expectedContent.substring(2000, 10801),
            expectedContent.substring(1500, 20001),
        };
        String[] uris = new String[10];
        for (int i = 0; i < 10; i++) {
            uris[i] = blobUri(digest);
        }
        assertThat(mget(uris, headers, expected), is(true));
    }

    @Test
    public void testHeadRequestConnectionIsNotClosed() throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        socket.setKeepAlive(true);
        socket.setSoTimeout(3000);

        OutputStream outputStream = socket.getOutputStream();
        outputStream.write("HEAD /_blobs/invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n"
            .getBytes(StandardCharsets.UTF_8));
        outputStream.write("Host: localhost\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        int linesRead = 0;
        while (linesRead < 3) {
            String line = reader.readLine();
            System.out.println(line);
            linesRead++;
        }

        assertSocketIsConnected(socket);
        outputStream.write("HEAD /_blobs/invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n"
            .getBytes(StandardCharsets.UTF_8));
        outputStream.write("Host: localhost\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        int read = reader.read();
        assertThat(read, greaterThan(-1));
        assertSocketIsConnected(socket);
    }

    @Test
    public void testResponseContainsCloseHeaderOnHttp10() throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        socket.setKeepAlive(false);
        socket.setSoTimeout(3000);

        OutputStream outputStream = socket.getOutputStream();
        outputStream.write("HEAD /_blobs/invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.0\r\n"
            .getBytes(StandardCharsets.UTF_8));
        outputStream.write("Host: localhost\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(socket.getInputStream(),
            StandardCharsets.UTF_8));
        String line;
        List<String> lines = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        assertThat(lines, hasItem("Connection: close"));
    }

    private void assertSocketIsConnected(Socket socket) {
        assertThat(socket.isConnected(), is(true));
        assertThat(socket.isClosed(), is(false));
        assertThat(socket.isInputShutdown(), is(false));
        assertThat(socket.isOutputShutdown(), is(false));
    }

    @Test
    public void testEmptyFile() throws IOException {
        CloseableHttpResponse res = put(blobUri("da39a3ee5e6b4b0d3255bfef95601890afd80709"), "");
        assertThat(res.getStatusLine().getStatusCode(), is(201));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Created"));

        res = put(blobUri("da39a3ee5e6b4b0d3255bfef95601890afd80709"), "");
        assertThat(res.getStatusLine().getStatusCode(), is(409));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Conflict"));
    }

    @Test
    public void testGetInvalidDigest() throws Exception {
        CloseableHttpResponse resp = get(blobUri("invlaid"));
        assertThat(resp.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testIndexOnNonBlobTable() throws IOException {
        // this test works only if ES API is enabled
        HttpPut httpPut = new HttpPut(String.format(Locale.ENGLISH, "http://%s:%s/test_no_blobs/default/1",
            address.getHostName(), address.getPort()));
        String blobData = String.format(Locale.ENGLISH, "{\"content\": \"%s\"}", StringUtils.repeat("a", 1024 * 64));
        httpPut.setEntity(new StringEntity(blobData, ContentType.APPLICATION_OCTET_STREAM));
        CloseableHttpResponse res = httpClient.execute(httpPut);
        assertThat(EntityUtils.toString(res.getEntity()),
            is("{\"_index\":\"test_no_blobs\",\"_type\":\"default\"," +
               "\"_id\":\"1\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},\"created\":true}"));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Created"));
        assertThat(res.getStatusLine().getStatusCode(), is(201));
    }
}
