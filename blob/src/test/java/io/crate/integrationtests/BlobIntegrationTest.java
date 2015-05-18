package io.crate.integrationtests;


import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 2)
public class BlobIntegrationTest extends BlobHttpIntegrationTest {

    private String uploadSmallBlob() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        put(blobUri(digest), StringUtils.repeat("a", 1500));
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
        assertEquals(400, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testNonExistingFile() throws IOException {
        CloseableHttpResponse response = get("test/d937ea65641c23fadc83616309e5b0e11acc5806");
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testUploadValidFile() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri(digest), StringUtils.repeat("a", 1500));
        assertEquals(201, response.getStatusLine().getStatusCode());
        /* Note that the content length is specified in the response in order to
        let keep alive clients know that they don't have to wait for data
        after the put and may close the connection if appropriate */
        assertEquals("0", response.getFirstHeader("Content-Length").getValue());
    }

    @Test
    public void testUploadChunkedWithConflict() throws IOException {
        String digest = uploadBigBlob();
        CloseableHttpResponse conflictRes = put(blobUri(digest), StringUtils.repeat("abcdefghijklmnopqrstuvwxyz", 1024 * 600));
        assertEquals(409, conflictRes.getStatusLine().getStatusCode());
    }

    @Test
    public void testUploadToUnknownBlobTable() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri("test_no_blobs", digest), StringUtils.repeat("a", 1500));
        assertEquals(400, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testGetFiles() throws IOException {
        String digest = uploadBigBlob();
        CloseableHttpResponse res = get(blobUri(digest));
        assertEquals(15974400, res.getEntity().getContentLength());
    }

    @Test
    public void testHeadRequest() throws IOException {
        String digest = uploadSmallBlob();
        CloseableHttpResponse res = head(blobUri(digest));
        assertEquals("1500", res.getFirstHeader("Content-Length").getValue());
        assertEquals("bytes", res.getFirstHeader("Accept-Ranges").getValue());
        assertEquals("Thu, 31 Dec 2037 23:59:59 GMT", res.getFirstHeader("Expires").getValue());
        assertEquals("max-age=315360000", res.getFirstHeader("Cache-Control").getValue());
    }

    @Test
    public void testRedirect() throws IOException {
        //One of the head requests must be redirected::

        String digest = uploadSmallBlob();
        int numberOfRedirects = getNumberOfRedirects(blobUri(digest), address);

        int numberOfRedirects2 = getNumberOfRedirects(blobUri(digest), address2);
        assertTrue(numberOfRedirects > 0 != numberOfRedirects2 > 0);
    }

    @Test
    public void testDeleteFile() throws IOException {
        String digest = uploadSmallBlob();
        String uri = blobUri(digest);
        CloseableHttpResponse res = delete(uri);
        assertEquals(204, res.getStatusLine().getStatusCode());

        res = get(uri);
        assertEquals(404, res.getStatusLine().getStatusCode());
    }

    @Test
    public void testByteRange() throws IOException {
        String digest = uploadTinyBlob();
        Header[] headers = {
          new BasicHeader("Range", "bytes=8-")
        };
        CloseableHttpResponse res = get(blobUri(digest), headers);
        assertEquals("18", res.getFirstHeader("Content-Length").getValue());
        assertEquals("bytes 8-25/26", res.getFirstHeader("Content-Range").getValue());
        assertEquals("bytes", res.getFirstHeader("Accept-Ranges").getValue());
        assertEquals("Thu, 31 Dec 2037 23:59:59 GMT", res.getFirstHeader("Expires").getValue());
        assertEquals("max-age=315360000", res.getFirstHeader("Cache-Control").getValue());
        assertEquals("ijklmnopqrstuvwxyz", EntityUtils.toString(res.getEntity()));

        res = get(blobUri(digest),new Header[] {
                new BasicHeader("Range", "bytes=0-1")
        });
        assertEquals("ab", EntityUtils.toString(res.getEntity()));

        res = get(blobUri(digest),new Header[] {
                new BasicHeader("Range", "bytes=25-")
        });
        assertEquals("z", EntityUtils.toString(res.getEntity()));
    }

    @Test
    public void testInvalidByterange() throws IOException {
        String digest = uploadTinyBlob();
        Header[] headers = {
                new BasicHeader("Range", "bytes=40-58")
        };
        CloseableHttpResponse res = get(blobUri(digest), headers);
        assertEquals(416, res.getStatusLine().getStatusCode());
        assertEquals("Requested Range Not Satisfiable", res.getStatusLine().getReasonPhrase());
        assertEquals("0", res.getFirstHeader("Content-Length").getValue());
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
        assertEquals(true, mget(uris, headers, expected));
    }

    @Test
    public void testParallelAccessWithRange() throws Throwable {
        String digest = uploadBigBlob();
        String expectedContent = StringUtils.repeat("abcdefghijklmnopqrstuvwxyz", 1024 * 600);
        Header[][] headers = new Header[][] {
                { new BasicHeader("Range", "bytes=0-") },
                { new BasicHeader("Range", "bytes=10-100") },
                { new BasicHeader("Range", "bytes=20-30") },
                { new BasicHeader("Range", "bytes=40-50") },
                { new BasicHeader("Range", "bytes=40-80") },
                { new BasicHeader("Range", "bytes=10-80") },
                { new BasicHeader("Range", "bytes=5-30") },
                { new BasicHeader("Range", "bytes=15-3000") },
                { new BasicHeader("Range", "bytes=2000-10800") },
                { new BasicHeader("Range", "bytes=1500-20000") },
        };
        String[] expected = new String[] {
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
        assertEquals(true, mget(uris, headers, expected));
    }

    @Test
    public void testEmptyFile() throws IOException {
        CloseableHttpResponse res = put(blobUri("da39a3ee5e6b4b0d3255bfef95601890afd80709"), "");
        assertEquals(res.getStatusLine().getStatusCode(), 201);
        assertEquals(res.getStatusLine().getReasonPhrase(), "Created");

        res = put(blobUri("da39a3ee5e6b4b0d3255bfef95601890afd80709"), "");
        assertEquals(res.getStatusLine().getStatusCode(), 409);
        assertEquals(res.getStatusLine().getReasonPhrase(), "Conflict");
    }

    @Test
    public void testIndexOnNonBlobTable() throws IOException {
        // this test works only if ES API is enabled
        HttpPut httpPut = new HttpPut(String.format(Locale.ENGLISH, "http://%s:%s/test_no_blobs/default/1",
                address.getHostName(), address.getPort()));
        String blobData = String.format("{\"content\": \"%s\"}", StringUtils.repeat("a", 1024 * 64));
        httpPut.setEntity(new StringEntity(blobData, ContentType.APPLICATION_OCTET_STREAM));
        CloseableHttpResponse res = httpClient.execute(httpPut);
        assertEquals(201, res.getStatusLine().getStatusCode());
        assertEquals("Created", res.getStatusLine().getReasonPhrase());
        assertEquals("{\"_index\":\"test_no_blobs\",\"_type\":\"default\",\"_id\":\"1\",\"_version\":1,\"created\":true}",
                EntityUtils.toString(res.getEntity()));
    }
}
