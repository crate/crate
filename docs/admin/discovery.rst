.. _node_discovery:

===============
Cloud discovery
===============

.. rubric:: Table of contents

.. contents::
   :local:

Amazon EC2 discovery
====================

CrateDB has native discovery support when running a cluster with *Amazon Web
Services* (AWS). The discovery mechanism uses the `Amazon EC2`_ API to to
generate the list of hosts for the unicast host discovery (see
:ref:`conf_host_discovery`).

There is a `best practice`_ how to configure and run a CrateDB
cluster on Amazon EC2.

.. _`Amazon EC2`: https://aws.amazon.com/ec2
.. _`EC2 API`: https://docs.aws.amazon.com/AWSEC2/latest/APIReference/Welcome.html
.. _best practice: https://crate.io/docs/crate/guide/best_practices/ec2_setup.html

.. _azure_discovery:

Microsoft Azure discovery
=========================

CrateDB has native discovery support when running a cluster on `Microsoft
Azure`_ infrastructure. The discovery mechanism uses the *Azure Resource
Management* (ARM) API to generate the list of hosts for the unicast host
discovery (see :ref:`conf_host_discovery`).

The discovery mechanism is implemented as a plugin and resides within the
plugin folder of the CrateDB installation. However, since the Azure Java SDKs
does have a lot of dependencies and we want to keep the plugin jar small in
size the dependencies are not included in the CrateDB distribution.

Requirements
------------

To make the plugin work you have to add the following Java libraries to the
``$CRATE_HOME/plugins/crate-azure-discovery`` folder::

    activation-1.1.jar
    adal4j-1.0.0.jar
    azure-core-0.9.3.jar
    azure-mgmt-compute-0.9.3.jar
    azure-mgmt-network-0.9.3.jar
    azure-mgmt-resources-0.9.3.jar
    azure-mgmt-storage-0.9.3.jar
    azure-mgmt-utility-0.9.3.jar
    bcprov-jdk15on-1.51.jar
    commons-io-2.4.jar
    commons-lang-2.6.jar
    commons-lang3-3.3.1.jar
    gson-2.2.4.jar
    jackson-core-asl-1.9.2.jar
    jackson-jaxrs-1.9.2.jar
    jackson-mapper-asl-1.9.2.jar
    jackson-xc-1.9.2.jar
    javax.inject-1.jar
    jaxb-api-2.2.2.jar
    jaxb-impl-2.2.3-1.jar
    jcip-annotations-1.0.jar
    jersey-client-1.13.jar
    jersey-core-1.13.jar
    jersey-json-1.13.jar
    jettison-1.1.jar
    json-smart-1.1.1.jar
    lang-tag-1.4.jar
    mail-1.4.7.jar
    nimbus-jose-jwt-3.1.2.jar
    oauth2-oidc-sdk-4.5.jar
    stax-api-1.0-2.jar

You can download the libraries using a simple ``build.gradle`` file:

.. code-block:: groovy

    apply plugin: "java"

    repositories {
      jcenter()
    }

    dependencies {
        compile('com.microsoft.azure:azure-mgmt-utility:0.9.3') {
            exclude group: 'stax', module: 'stax-api'
            exclude group: 'org.slf4j', module: 'slf4j-api'
            exclude group: 'commons-logging', module: 'commons-logging'
            exclude group: 'commons-codec', module: 'commons-codec'
            exclude group: 'com.fasterxml.jackson.core', module: 'jackson-core'
            exclude group: 'org.apache.httpcomponents', module: 'httpclient'
        }
    }

    task azureLibs (type: Copy, dependsOn: ['compileJava']) {
      from configurations.testRuntime
      into "libs"
    }

Running ``gradle azureLibs`` will fetch the required jars and put them into the
``libs/`` folder from where you can copy them into the plugin folder.

Basic configuration
-------------------

To enable Azure discovery simply change the ``discovery.seed_providers``
setting to ``azure``::

    discovery.seed_providers: azure

The discovery mechanism can discover CrateDB instances within the same **vnet**
or the same **subnet** of the same **resource group**. By default it will the
**vnet**.

You can change the behaviour using the ``discovery.azure.method`` setting::

    discovery.azure.method: subnet

The used resource group also needs to be provided::

    cloud.azure.management.resourcegroup.name: production

Authentication
--------------

The discovery plugin requires authentication as service principle. To do so,
you have to create an ``Active Directory`` application with a password. We
recommend to follow the `AD Application Guide`_ .

The configuration settings for authentication are as follows:

.. code-block:: yaml

    cloud.azure.management:
      subscription.id: my-id
      tenant.id: my-tenant
      app:
        id: my-app
        secret: my-secret

For a complete list of settings please refer to :ref:`conf_azure_discovery`.

.. _`Microsoft Azure`: https://azure.microsoft.com
.. _`AD Application Guide`: https://azure.microsoft.com/en-us/documentation/articles/resource-group-authenticate-service-principal-cli/
