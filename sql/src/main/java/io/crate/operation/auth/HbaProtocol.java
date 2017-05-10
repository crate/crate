package io.crate.operation.auth;

public enum HbaProtocol {

    POSTGRES("pg");

    private final String protocolName;

    HbaProtocol(String protocolName) {
        this.protocolName = protocolName;
    }

    @Override
    public String toString() {
        return protocolName;
    }
}
