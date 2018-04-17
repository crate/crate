.. highlight:: sh

.. _conf-secure-settings:

===============
Secure Settings
===============

Some settings require additional protection. For example: passwords, access 
keys, and so on. When handling this type of data, it is typically best practice 
not to rely on file system permissions, and not to store it on disk using plain 
text.

For sensitive configuration data, we provide a keystore_ along with the 
``crate-keystore`` tool to manage the data in the keystore_.

.. NOTE::

    All commands here should be run as the user which will run CrateDB.

.. NOTE::

    Changes to the keystore only take effect after restarting CrateDB.

.. NOTE::

    The CrateDB keystore currently provides only obfuscation, as password
    protection is not currently implemented.

.. NOTE::

    Not all settings are read from the keystore. Consult the documentation for 
    each setting to see if it is secure setting.

Creating the Keystore
---------------------

To create the ``crate.keystore`` file, use the ``create`` argument:

.. code-block:: sh

    sh$ ./bin/crate-keystore create

The file ``crate.keystore`` will be created alongside ``crate.yml`` in the 
CrateDB :ref:`configuration directory <config-directory>`.

Listing Settings in the Keystore
--------------------------------

To list all the settings in the keystore, use the ``list`` argument:

.. code-block:: sh

    sh$ ./bin/crate-keystore list

Adding String Settings
----------------------

Sensitive settings, like authentication credentials for cloud discovery, can be
added using the ``add`` argument:


.. code-block:: sh

    sh$ ./bin/crate-keystore add setting.name

The script will then prompt for the value of the setting. To pass the value 
through stdin, use the ``--stdin`` flag:

.. code-block:: sh

    sh$ cat /file/containing/setting/value | ./bin/crate-keystore add --stdin setting.name

Removing Settings
-----------------

To remove a setting from the keystore, use the ``remove`` argument:

.. code-block:: sh

    sh$ ./bin/crate-keystore remove setting.name

.. _keystore: https://docs.oracle.com/javase/8/docs/api/java/security/KeyStore.html 
.. _PBE: https://docs.oracle.com/javase/8/docs/api/javax/crypto/spec/PBEKeySpec.html
