#!/bin/bash
set -e
./gen_root_ca.sh capass tspass
./gen_node_cert.sh 0 kspass capass && ./gen_node_cert.sh 1 kspass capass &&  ./gen_node_cert.sh 2 kspass capass