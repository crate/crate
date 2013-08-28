package crate.elasticsearch.doctests;

import org.elasticsearch.common.settings.Settings;

import com.github.tlrx.elasticsearch.test.EsSetup;

public class StoreEsSetup extends EsSetup {

    public StoreEsSetup() {
        super(new StoreLocalClientProvider());
    }

    public StoreEsSetup(Settings settings) {
        super(new StoreLocalClientProvider(settings));
    }
}
