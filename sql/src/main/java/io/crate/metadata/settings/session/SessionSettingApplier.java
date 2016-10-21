package io.crate.metadata.settings.session;

import io.crate.action.sql.SessionContext;

public interface SessionSettingApplier {

    void apply(String s, SessionContext sessionContext);
}
