package crate.bootstrap;

import org.elasticsearch.bootstrap.Bootstrap;

public class CrateF extends Bootstrap {

    public static void close(String[] args) {
        Bootstrap.close(args);
    }

    public static void main(String[] args) {
        System.setProperty("es.foreground", "yes");
        Bootstrap.main(args);
    }
}
