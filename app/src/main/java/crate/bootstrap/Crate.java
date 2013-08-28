package crate.bootstrap;

import org.elasticsearch.bootstrap.Bootstrap;

public class Crate extends Bootstrap {

    public static void close(String[] args) {
        Bootstrap.close(args);
    }

    public static void main(String[] args) {
        Bootstrap.main(args);
    }
}
