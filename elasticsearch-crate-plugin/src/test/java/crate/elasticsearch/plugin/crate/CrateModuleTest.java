package crate.elasticsearch.plugin.crate;

import com.github.tlrx.elasticsearch.test.EsSetup;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;

public class CrateModuleTest {

    protected EsSetup esSetup;

    private void doSetUp() {
        esSetup = new EsSetup();
        esSetup.execute(createIndex("test"));
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    /**
     * Deleting all indexes must be deactivated by default
     */
    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testDeleteAll() {
        doSetUp();
        esSetup.execute(deleteAll());
    }


}