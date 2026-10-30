# DotPulsar

The official .NET client library for [Apache Pulsar](https://pulsar.apache.org/).

DotPulsar is written entirely in C# and implements Apache Pulsar's [binary protocol](https://pulsar.apache.org/docs/en/develop-binary-protocol/).

## What's new?

Have a look at the [changelog](https://github.com/apache/pulsar-dotpulsar/blob/master/CHANGELOG.md).

## Getting Started

Let's take a look at a "Hello world" example, where we first produce a message and then consume it. Note that the topic and subscription will be created if they don't exist.

First, we need a Pulsar setup. See [Pulsar docs](https://pulsar.apache.org/docs/getting-started-home/) for how to set up a local standalone Pulsar instance.

Install the NuGet package [DotPulsar](https://www.nuget.org/packages/DotPulsar/) and run the follow code example:

```csharp
using DotPulsar;
using DotPulsar.Extensions;

const string myTopic = "persistent://public/default/mytopic";

// connecting to pulsar://localhost:6650
await using var client = PulsarClient.Builder().Build();

// produce a message
await using var producer = client.NewProducer(Schema.String).Topic(myTopic).Create();
await producer.Send("Hello World");

// consume messages
await using var consumer = client.NewConsumer(Schema.String)
    .SubscriptionName("MySubscription")
    .Topic(myTopic)
    .InitialPosition(SubscriptionInitialPosition.Earliest)
    .Create();

await foreach (var message in consumer.Messages())
{
    Console.WriteLine($"Received: {message.Value()}");
    await consumer.Acknowledge(message);
}
```

For a more in-depth tour of the API, please visit the [Wiki](https://github.com/apache/pulsar-dotpulsar/wiki).

## Transactions

A transaction makes a group of sends and acknowledgments atomic: either all of them take effect or none of them do. Transactions must be enabled on the client (the connection to the transaction coordinator is created when the first transaction is created), and the broker must have `transactionCoordinatorEnabled=true`.

```csharp
await using var client = PulsarClient.Builder().EnableTransaction(true).Build();

await using var consumer = client.NewConsumer(Schema.String)
    .SubscriptionName("MySubscription")
    .Topic("persistent://public/default/input")
    .Create();

await using var producer = client.NewProducer(Schema.String)
    .Topic("persistent://public/default/output")
    .Create();

await foreach (var message in consumer.Messages())
{
    var transaction = await client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create();

    try
    {
        // Nothing is visible to other consumers, and the consumed message stays unacknowledged, until the transaction is committed
        var send = producer.NewMessage().Transaction(transaction).Send(message.Value() + "-processed");
        var acknowledge = consumer.Acknowledge(message.MessageId, transaction);

        // Waits for the send and the acknowledgment, then commits. If either failed, the transaction is aborted and an exception is thrown.
        await transaction.Commit();

        await send;
        await acknowledge;
    }
    catch (Exception)
    {
        await transaction.Abort();
    }
}
```

See the [Transactions sample](https://github.com/apache/pulsar-dotpulsar/tree/master/samples/Transactions) for a complete program.

## Batching and large messages

A producer can send several messages to the broker as one batch, which is faster and uses less memory for small messages. Batching is off by default. A batch is sent when it has `BatchingMaxMessages` messages, `BatchingMaxBytes` bytes, or when `BatchingMaxPublishDelay` has passed. `Flush` sends the batch at once.

```csharp
await using var producer = client.NewProducer(Schema.String)
    .Topic("persistent://public/default/mytopic")
    .EnableBatching(true)
    .BatchingMaxMessages(1000)
    .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(10))
    .SendTimeout(TimeSpan.FromSeconds(30)) // fail the sends that the broker has not acknowledged in time (default: no timeout)
    .Create();

var sends = Enumerable.Range(0, 100).Select(i => producer.Send($"message {i}").AsTask()).ToArray();
await producer.Flush();
await Task.WhenAll(sends);
```

A message that is larger than the maximum message size of the broker is rejected with a `TooLargeMessageException`, unless chunking is enabled. A producer that has chunking enabled splits such a message in chunks, and a consumer or a reader puts the chunks together and delivers one message. Chunking can not be combined with batching.

```csharp
await using var producer = client.NewProducer(Schema.ByteArray)
    .Topic("persistent://public/default/mytopic")
    .EnableChunking(true)
    .Create();
```

## End-to-end encryption

A producer can encrypt its messages, so that the broker never sees them in clear text, and a consumer or a reader decrypts them. The producer encrypts every message (a batch, or every chunk of a chunked message) with a data key that it wraps with the public key of each encryption key. The consumer unwraps the data key with the private key of the same name. RSA keys need .NET Standard 2.1 or later and EC keys need .NET 8 or later.

```csharp
await using var producer = client.NewProducer(Schema.String)
    .Topic("persistent://public/default/mytopic")
    .DefaultCryptoKeyReader(new Dictionary<string, string> { ["my-app-key"] = "file:/keys/my-app-key.public.pem" })
    .EncryptionKey("my-app-key")
    .Create();

await using var consumer = client.NewConsumer(Schema.String)
    .Topic("persistent://public/default/mytopic")
    .SubscriptionName("my-subscription")
    .DefaultCryptoKeyReader(new Dictionary<string, string> { ["my-app-key"] = "file:/keys/my-app-key.private.pem" })
    .Create();
```

You can implement `ICryptoKeyReader` to get the keys from somewhere else. `CryptoFailureAction` selects what happens when a message can not be encrypted (the producer fails the send, or sends the message unencrypted) or decrypted (the consumer fails the message, discards it, or delivers it encrypted). A consumer that has no key for an encrypted message fails the message by default, it used to deliver the encrypted payload.

## Supported features

- Service discovery
- Automatic reconnect
- TLS connections
- Pulsar Proxy
- Producer - send with custom metadata
- Producer - send with event time, sequence id, and delayed message delivery
- Producer - send with key and ordering key
- Producer - partitioned topics
- Consumer - subscription with initial position and priority level
- Consumer - subscription types exclusive, shared, failover, and key shared
- Consumer - receive and single + cumulative acknowledge
- Consumer - acknowledgment receipts and batch index acknowledgment
- Consumer - ack timeout and negative acknowledgment
- Consumer - dead letter topic
- Consumer/Reader - seek on message-id and publish time
- Consumer - unsubscribe
- Consumer - compacted topics
- Reader API
- Read/Consume/Acknowledge batched messages
- Producer - batching of messages and flush
- Producer - send timeout
- Producer/Consumer/Reader - chunking of messages that are larger than the maximum message size of the broker
- Producer/Consumer/Reader - end-to-end message encryption with RSA and ECDSA keys
- Transactions - produce and acknowledge (single and cumulative) in a transaction
- Telemetry
    - [Tracing](https://github.com/apache/pulsar-dotpulsar/wiki/Tracing)
    - [Metrics](https://github.com/apache/pulsar-dotpulsar/wiki/Metrics)
- Authentication
    - TLS Authentication
    - JSON Web Token Authentication
    - Custom Authentication
- [Message compression](https://github.com/apache/pulsar-dotpulsar/wiki/Compression)
    - LZ4
    - ZLIB
    - ZSTD
    - SNAPPY
- Schemas
    - Boolean
    - Bytes (using `byte[]` and `ReadOnlySequence<byte>`)
    - String (UTF-8, UTF-16, and US-ASCII)
    - INT8, INT16, INT32, and INT64
    - Float and Double
    - Time (using TimeSpan)
    - Timestamp and Date (using DateTime)

For a horizontal comparison with more language-specific clients, see [Client Feature Matrix](https://pulsar.apache.org/client-feature-matrix/).

## Join Our Community

Apache Pulsar has a [Slack instance](https://pulsar.apache.org/contact/), and there you'll find us in the #dev-dotpulsar channel.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/apache/pulsar-dotpulsar/tags).

## Contribution

* **Daniel Blankensteiner** - *Initial work* - [Danske Commodities](https://github.com/DanskeCommodities)

Contributions are welcomed and greatly appreciated. See also the list of [contributors](https://github.com/apache/pulsar-dotpulsar/contributors) who participated in this project. Read the [CONTRIBUTING](https://github.com/apache/pulsar-dotpulsar/blob/master/CONTRIBUTING.md) guide for how to participate.

If your contribution adds Pulsar features for C# clients, you need to update both the [Pulsar docs](https://pulsar.apache.org/docs/client-libraries/) and the [Client Feature Matrix](https://pulsar.apache.org/client-feature-matrix/). See [Contribution Guide](https://pulsar.apache.org/contribute/site-intro/#pages) for more details.

## License

This project is licensed under [Apache License, Version 2.0](https://apache.org/licenses/LICENSE-2.0).
