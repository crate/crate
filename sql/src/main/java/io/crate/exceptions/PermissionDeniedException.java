package io.crate.exceptions;

import io.crate.analyze.user.Privilege;
import io.crate.operation.user.User;

import java.util.Locale;

public class PermissionDeniedException extends UnauthorizedException {

    private static final String MESSAGE_TMPL = "Missing '%s' Privilege for user '%s'";

    public PermissionDeniedException(String userName, Privilege.Type type) {
        super(String.format(Locale.ENGLISH, MESSAGE_TMPL, type, userName));
    }

    @Override
    public int errorCode() {
        return 1;
    }

}
