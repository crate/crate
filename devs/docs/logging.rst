=======
Logging
=======


Debug and Trace Logging
=======================

With asynchronous programming models, it is often hard to follow the execution
flow or identify possible bugs. This is especially true for distributed systems.

Log messages can make this easier. When implementing new features, refactoring
code, or debugging, you are encouraged to add (or improve) log messages. This
is likely to help you with your current work, as well as other people in the
future.

Such messages should be logged at the ``DEBUG`` or ``TRACE`` log level,
depending on how low-level the information is. To avoid introducing additional
performance overheads, when the logging statements include some relatively
expensive computation or they use more than 10 arguments, they must also be
conditional on the current level of the logger. For example::

  if (logger.isTraceEnabled()) {
      // Relatively expensive calculations
      ...
      logger.trace("My useful log message");
  }

These sorts of function calls (``isFooEnabled``) are relatively expensive
themselves. If possible, save the result to a class attribute during its
initialization.

For components of the distributed execution engine (i.e.:
``PageDownstreamContext``), additional node information is very useful. To
do this, instead of using a ``static`` logger instance, make the logger
instance a class attribute which is initialized with the ``Settings``.
For example::

  public class MyClass {

       private final Logger logger;

       @Inject
       public MyClass(Settings settings, ...) {
           logger = Loggers.getLogger(MyClass.class, settings);
           ...
        }
  ...
  }

In your log messages, try to include information about the environment in
which the operation takes place. For example, ``nodeOperations``,
``phaseId``, and so on. Please also consult the `logfmt`_ guidelines.


.. _logfmt: https://www.brandur.org/logfmt
