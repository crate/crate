.. _node_discovery:

===============
Cloud discovery
===============

Amazon EC2 discovery
====================

CrateDB has native discovery support when running a cluster with *Amazon Web
Services* (AWS). The discovery mechanism uses the `Amazon EC2`_ API to
generate the list of hosts for the unicast host discovery (see
:ref:`conf_host_discovery`).

There is a `best practice`_ how to configure and run a CrateDB
cluster on Amazon EC2.

.. _`Amazon EC2`: https://aws.amazon.com/ec2
.. _`EC2 API`: https://docs.aws.amazon.com/AWSEC2/latest/APIReference/Welcome.html
.. _best practice: https://crate.io/docs/crate/howtos/en/latest/deployment/cloud/aws/ec2-setup.html
