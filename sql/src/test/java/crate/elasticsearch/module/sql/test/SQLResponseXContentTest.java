package crate.elasticsearch.module.sql.test;

import crate.elasticsearch.action.sql.SQLResponse;
import junit.framework.TestCase;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;

public class SQLResponseXContentTest extends TestCase {

    private XContentBuilder builder() throws IOException {
        return XContentFactory.contentBuilder(XContentType.JSON);
    }

    private String json(ToXContent x) throws IOException {
        return x.toXContent(builder(), ToXContent.EMPTY_PARAMS).string();
    }

    @Test
    public void testInt() throws Exception {
        SQLResponse r = new SQLResponse();
        r.cols(new String[]{"col1", "col2"});
        r.rows(new Object[][]{new Object[]{1, 2}});

        //System.out.println(json(r));
        JSONAssert.assertEquals(
                "{\"cols\":[\"col1\",\"col2\"],\"rows\":[[1,2]]}",
                json(r), false);
    }

    @Test
    public void testString() throws Exception {
        SQLResponse r = new SQLResponse();
        r.cols(new String[]{"some", "thing"});
        r.rows(new Object[][]{
                new Object[]{"one", "two"},
                new Object[]{"three", "four"},
        });
        //System.out.println(json(r));
        JSONAssert.assertEquals(
                "{\"cols\":[\"some\",\"thing\"],\"rows\":[[\"one\",\"two\"],[\"three\",\"four\"]]}",
                json(r), false);
    }


}
