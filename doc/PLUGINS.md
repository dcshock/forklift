### Plugins
Forklift has been thoughtfully designed enough to allow developers to
extend the base functionality for message processing. One of the major
requirements when originally developing this system was the ability to
log/audit every step of the life-cycle. However, every developer has
different logging requirements, whether that be logging to a file or sending
all logs to a database table. So integrating a single solution for auditing
did not make sense.

Several plug-ins have been developed that may be useful to other developers
for general use or as a template to build their own. These are part of
the Forklift project and are described below.

#### Specialized Message Handling

##### Retrying Messages
There are times when a consumer may not be able to successfully complete
processing a message. Say for example that a message is placed on a queue that
feeds a consumer data that is to be pushed to a remote web-service. Of course
the consumer could write all the code to send the message, catch any exceptions
and then retry if it fails. Problem is, threads get bogged down doing retries
and there is no record of failed attempts. The retry message handler adds an
annotation that can be placed on the consumer that instructs Forklift on failure of
consuming a message to schedule to retry processing the message a set number of
times after a specified delay.

* [`@Retry`](../plugins/replay/src/main/java/forklift/replay/Replay.java) - Placed
on the consumer class will tell Forklift to reschedule the message to be rerun in
the case of errors.
_Fields_:
   * `maxRetries` - the maximumum number of times a message should be retried
   * `timeout` - the amount of time in between retries, in seconds, default value is
12 hours
   * `role` - the name of the role of the consumer to write retry messages to, used
as a fallback if there is no [`RoleInputSource`](../core/src/main/java/forklift/source/sources/RoleInputSource.java)
specified on the consumer

##### Replay Auditing
One of the joys of auditing is seeing what actually happened when the system
executes. Being able to view the entire life-cycle on every message that
goes through a system provides some with a great sense of comfort. In the
case when a consumer errors, it may be for a reason outside the consumer's
control. In this case just being able to resend the message as it originally
existed, may allow it to process. The replay auditor provides a log of
every life-cycle event for each annotated consumer that can then be parsed
and used to recreate an event.

* [`@Replay`](../plugins/replay/src/main/java/forklift/replay/Replay.java) - Placed on the consumer class will log out all the life-cycle
events to a log file with enough data to be able to replay the message. In
essence making it possible to resend a message that has already been processed.
_Fields_:
    * `role` - the name of the role of the consumer to write replay messages to, used
as a fallback if there is no [`RoleInputSource`](../core/src/main/java/forklift/source/sources/RoleInputSource.java)
specified on the consumer

##### Message Processing Statistics
The [stats collector plugin](../plugins/stats/) appends data about the timing of each life-cycle
event to the properties of the corresponding message, which can then later
be persisted by another plugin. For example, this works really well in
conjunction with the replay plugin which stores this information in a
datastore like elastic search.

### Resending Messages to a Consumer
To be able to reliably and safely allow consumers to re-process messages, the concept of a _role_
was introduced to forklift. Essentially, this is a unique way to identify a consumer so that it can
be specifically targeted for reprocessing messages.

To specify that a consumer can be resent messages, add the [`@RoleInput`](../core/src/main/java/forklift/source/decorators/RoleInput.java)
annotation to it. A role can be manually specified like so: `@RoleInput(role = "BestConsumer")`,
or if no role is specified, the name of the class will be used.

For example, the following class will have the role `TestConsumer`.
```java
@RoleInput
public class TestConsumer {
...
}
```

Adding this annotation means that an extra consumer (and set of threads) will be started to consume from a dedicated
source with the format `forklift-role-$CONSUMER_ROLE`.

#### Adding Plugins to a Forklift Server
To add a plugin to forklift, outside of the default setup provided by the forklift server project,
it is necessary to set the access the `LifecycleEvents` object provided by the `Forklift` instance,
and then call `register` with either a plugin class (for `StatsCollector`) or a
plugin instance (for either `RetryES` or `ReplayES`).

For example, the following code will start an embeded forklift instance connecting to ActiveMQ,
and then register the stats collector and replay plugins:
```java
final Forklift forkliftServer = new Forklift();
final ForkliftConnectorI connector = new ActiveMQConnector(activeMqBrokerUrl);
final ReplayES replayPlugin = new ReplayES(
  replayServer,
  elasticSearchHost,
  elasticSearchPort,
  elasticSearchCluster,
  connector
);

forkliftServer.getLifeCycle().register(StatsCollector.class);
forkliftServer.getLifeCycle().register(replayPlugin);
```

For more information on instantiating plugins, look at the javadoc of the associated plugin classes.

#### Designing Plugins for Forklift
A plugin class should have either instance or static methods which are annotated using the
`@LifeCycle` annotation, with a `ProcessStep` and optionally an annotation class specified.

These annotated methods will be discovered by the `LifeCycleMontiors` instance when the plugin is
registered. When a message enters a particular stage of its lifecycle, any methods which specify that
step as a lifecycle callback will be called with the `MessageRunnable` used for consuming that message.

If an annotation is specified by the `LifeCycle` annotatation, then that method will be called only if
the given annotation annotates the consumer class, and that annotation will be passed to the given method
as the second parameter.

For example, the following plugin can be used to statically produce a special log message whenever a message
errors.
```java
public class TestPlugin {
    private static final Logger log = LoggerFactory.getLogger(TestPlugin.class);

    @LifeCycle(ProcessStep.Error)
    public static void handleError(MessageRunnable mr) {
        final Consumer consumer = mr.getConsumer();
        final List<String> errors = mr.getErrors();
        final Class<?> messageHandler = consumer.getMsgHandler();
        final SourceI messageSource = consumer.getSource();

        log.error("Error consuming from " + source + " for consumer" + messageHandler.getSimpleName() + ": " + errors);
    }
}
```

Or, given the following annotation:
```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Foo {
    String value();
}
```

The following plugin can proccess consumers with the `Foo` annotation:
```java
public class FooPlugin {
    private static final Logger log = LoggerFactory.getLogger(FooPlugin.class);

    @LifeCycle(step = ProcessStep.Validating, annotation = Foo.class)
    public void handleFooValidating(MessageRunnable mr, Foo foo) {
        log.info("Validating: " + foo.value());
    }
}
```