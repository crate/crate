=======
Logging
=======

Debug and Trace Logging
.......................

When working with asynchronous programming, it is often hard to follow the
execution flow or identify possible bugs. This is especially true for
distributed systems.

Log messages can make this easier. Therefore, when implementing new features,
refactoring code, or even just debugging, you are encouraged to add
(or improve) log messages. This is likely to help you with your current work,
as well as helping other people with future work.

Such messages should be logged using the ``DEBUG`` or ``TRACE`` log level,
depending on how low-level the information is. To avoid introducing additional
performance overheads, when the logging statements include some relatively
expensive computation or they use more than 10 arguments, they must also be
conditional on the current level of the logger, e.g.::

  if (logger.isTraceEnabled()) {
      // Relatively expensive calculations
      ...
      logger.trace("My useful log message");
  }

These sorts of function calls (``isFooEnabled``) are relatively expensive
themselves, so if possible, save the result to a class attribute during
it's initialisation.

For components of the distributed execution engine (e.g.:
``PageDownstreamContext``), additional node information is very useful. To
achieve that, do not use a ``static`` logger instance, as usual. Instead, the
logger instance should become a class attribute which is initialized with the
``Settings``, e.g.::

  public class MyClass {

       private final Logger logger;

       @Inject
       public MyClass(Settings settings, ...) {
           logger = Loggers.getLogger(MyClass.class, settings);
           ...
        }
  ...
  }

In your log messages, try to include information about the "environment" in
which the operation takes place. For example, the ``nodeOperations``,
``phaseId``, and so on. Please also consult the `logfmt`_ guidelines.

.. _logfmt: https://www.brandur.org/logfmt
