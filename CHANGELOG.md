# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Support for acknowledgment receipts using ConsumerOptions.AckReceiptEnabled and IConsumerBuilder.AckReceiptEnabled
- Support for transactions using IPulsarClient.CreateTransaction and IPulsarClient.NewTransaction()
- Support for sending messages in a transaction using IMessageBuilder.Transaction and MessageMetadata.Transaction
- Support for acknowledging messages in a transaction using the IConsumer.Acknowledge and IConsumer.AcknowledgeCumulative overloads that take an ITransaction
- Support for batch index acknowledgment using ConsumerOptions.BatchIndexAckEnabled and IConsumerBuilder.BatchIndexAckEnabled
- Support for ack timeout using ConsumerOptions.AckTimeout, ConsumerOptions.AckTimeoutTickTime, IConsumerBuilder.AckTimeout and IConsumerBuilder.AckTimeoutTickTime
- Support for delaying the redelivery of the messages that are not acknowledged within the ack timeout using ConsumerOptions.AckTimeoutRedeliveryBackoff and IConsumerBuilder.AckTimeoutRedeliveryBackoff. The delay comes from an IRedeliveryBackoff, which is given the number of times a message was redelivered, and MultiplierRedeliveryBackoff is the one that multiplies the delay for every redelivery. A message is redelivered when the ack timeout and then the delay have passed
- Support for negative acknowledgment using IConsumer.NegativeAcknowledge, ConsumerOptions.NegativeAcknowledgeRedeliveryDelay and IConsumerBuilder.NegativeAcknowledgeRedeliveryDelay
- Support for a dead letter topic using DeadLetterPolicy, ConsumerOptions.DeadLetterPolicy and IConsumerBuilder.DeadLetterPolicy. A message that was received as chunks is sent to the dead letter topic as chunks
- Support for receiving chunked messages as one message using ConsumerOptions.MaxPendingChunkedMessage, ConsumerOptions.AutoAckOldestChunkedMessageOnQueueFull, ConsumerOptions.ExpireTimeOfIncompleteChunkedMessage, ReaderOptions.MaxPendingChunkedMessage, ReaderOptions.AutoAckOldestChunkedMessageOnQueueFull, ReaderOptions.ExpireTimeOfIncompleteChunkedMessage and the matching methods of IConsumerBuilder and IReaderBuilder
- MessageId.FirstChunkMessageId, which is the id of the first chunk of a chunked message
- Support for decrypting end-to-end encrypted messages in consumers and readers using ConsumerOptions.CryptoKeyReader, ConsumerOptions.CryptoFailureAction, ReaderOptions.CryptoKeyReader, ReaderOptions.CryptoFailureAction and the matching methods of IConsumerBuilder and IReaderBuilder. RSA keys require .NET Standard 2.1 or later and EC keys require .NET 8 or later. On .NET Standard 2.0 a message cannot be decrypted, so the CryptoFailureAction is used
- ICryptoKeyReader, EncryptionKeyInfo and DefaultCryptoKeyReader for reading keys, and the DefaultCryptoKeyReader extension methods of IConsumerBuilder and IReaderBuilder that read private keys from 'file:' and 'data:' URIs
- ConsumerCryptoFailureAction, which selects what a consumer or a reader does with a message that it cannot decrypt. The messages can be failed (the default), discarded or consumed encrypted
- IMessage.EncryptionContext, EncryptionContext and EncryptionKey, which describe how a message was encrypted
- Support for end-to-end encryption of the messages of a producer using ProducerOptions.CryptoKeyReader, ProducerOptions.EncryptionKeys, ProducerOptions.CryptoFailureAction and IProducerBuilder.CryptoKeyReader, IProducerBuilder.EncryptionKey and IProducerBuilder.CryptoFailureAction. A message, a batch and every chunk of a chunked message is encrypted after it has been compressed, and the chunks are made smaller by what the encryption adds to them. RSA keys require .NET Standard 2.1 or later and EC keys require .NET 8 or later. Encryption keys without a CryptoKeyReader are rejected with a ConfigurationException, where the Java client sends the messages unencrypted
- ProducerCryptoFailureAction, which selects what a producer does when it cannot encrypt a message. The send can fail (the default) or the message can be sent unencrypted
- The DefaultCryptoKeyReader extension methods of IProducerBuilder, which read public keys from 'file:' and 'data:' URIs
- CryptoException, which is thrown when a producer cannot read or use a public key (the creation of the producer fails) and when a message cannot be encrypted (only that send fails, and the producer is not faulted)
- Support for sending messages that are larger than the maximum message size of the broker as chunks using ProducerOptions.EnableChunking, ProducerOptions.ChunkMaxMessageSize, IProducerBuilder.EnableChunking and IProducerBuilder.ChunkMaxMessageSize. Chunking is only done on persistent topics and can not be combined with batching. The MessageId that is returned for a chunked message has a FirstChunkMessageId, and acknowledging it acknowledges all the chunks
- EnableTransaction and OperationTimeout on IPulsarClientBuilder
- Support for batching messages using ProducerOptions.EnableBatching, BatchingMaxMessages, BatchingMaxBytes, BatchingMaxPublishDelay and BatcherType, and the corresponding methods of IProducerBuilder
- IProducer.Flush, which sends the batched messages at once and waits for the acknowledgment of the last message
- Support for a send timeout using ProducerOptions.SendTimeout and IProducerBuilder.SendTimeout. The messages that the broker has not acknowledged in time fail with an OperationTimeoutException
- New exceptions: TransactionNotFoundException, TransactionHasOperationFailedException, TransactionCoordinatorClientException, TransactionCoordinatorClientStateException, MetaStoreHandlerNotExistsException, MetaStoreHandlerNotReadyException and OperationTimeoutException

### Changed

- Updated the protocol version from 14 to 19
- IPulsarClient, IPulsarClientBuilder, IConsumer, IConsumerBuilder and IMessageBuilder have new members. This is a breaking change for custom implementations of these interfaces
- IProducer and IProducerBuilder have new members. This is a breaking change for custom implementations of these interfaces
- PulsarClientClosedException has a new constructor that takes a message
- A send that the broker rejects with NotAllowedError now fails only that message with NotAllowedException instead of faulting the producer
- IConsumer.RedeliverUnacknowledgedMessages with an empty list of message ids no longer redelivers all messages, it does nothing
- IConsumer.RedeliverUnacknowledgedMessages with message ids on an Exclusive or Failover subscription redelivers all unacknowledged messages, because the broker only redelivers individual messages for Shared and KeyShared subscriptions
- IConsumer.RedeliverUnacknowledgedMessages sends the message ids without the batch index, and each entry only once, so all the messages of a batch are redelivered
- IConsumer.RedeliverUnacknowledgedMessages drops the messages that the consumer has received from the broker but not yet delivered, and gives the message permits back to the broker. On Exclusive and Failover subscriptions the messages that are on their way are dropped using a consumer epoch, which is now sent when subscribing
- IConsumer.Seek and IReader.Seek with the MessageId of a message in a batch now make the broker redeliver the batch from that message, so the messages of the batch before it are not redelivered. This needs the broker setting acknowledgmentAtBatchIndexLevelEnabled and the MessageId of a message that the consumer or reader received. Without the setting, or with the MessageId that a send returns or one that was parsed or created (they have no batch size), the whole batch is redelivered as before
- IReaderBuilder has new members. This is a breaking change for custom implementations of this interface
- IMessage has a new member. This is a breaking change for custom implementations of this interface
- A consumer or a reader no longer delivers a message that is encrypted by the producer if it cannot decrypt it, because it has no CryptoKeyReader or the key is wrong. It used to deliver the encrypted payload. Set CryptoFailureAction to ConsumerCryptoFailureAction.Consume to keep doing that
- A consumer or a reader that receives the chunks of a chunked message now delivers them as one message, where it used to deliver every chunk as a message. Acknowledging, negatively acknowledging or redelivering the message uses all its chunks, and a seek or a start of a reader at the message uses its first chunk

### Fixed

- A message that was larger than the maximum message size of the broker faulted the producer. Only the send of that message fails now, with a TooLargeMessageException, and the producer keeps sending
- A command from the broker that DotPulsar has no response handler for no longer ends the connection
- The protocol version and client version are now sent in the authentication response, so a multi-round initial authentication is no longer recorded by the broker as protocol version 0
- A cumulative acknowledgment of a partially acknowledged batch was dropped
- Batch acknowledgment trackers were not released after a cumulative acknowledgment
- Messages in a batch had a MessageId without the topic
- The ordering key of a message in a batch was taken from the batch instead of from the message
- The pooled acknowledgment command kept the MessageIdData instance of the broker
- A full send queue did not accept new messages after the producer removed messages from it
- Messages that the consumer had already received from the broker were received twice after IConsumer.RedeliverUnacknowledgedMessages
- A message that a consumer of a partitioned topic or of several topics had already received from one of the topics, but not yet delivered to the application, was delivered besides the redelivered message after IConsumer.RedeliverUnacknowledgedMessages
- The number of messages of the pooled send command was never reset
- A producer with a schema never put the schema version in its messages, so IMessage.SchemaVersion was null for every message that DotPulsar produced
- A consumer of a partitioned topic, of several topics or of a topics pattern reported the state Active as soon as the first of its topics was subscribed. It is now Active when every topic is subscribed, until then it is PartiallyConnected or Disconnected
- A consumer or a reader published the state Active or Connected when the broker had answered the subscribe command, before it had its channel, so a Receive that followed the state change could wait for the retry interval (3 seconds by default). The state is published when the channel is in place
- Disposing a producer, a consumer or a reader waited without a limit for the answer to the command that closes it, which was 91 seconds when the connection was dead but not detected yet. It waits as long as the OperationTimeout of the client (30 seconds by default) and then goes on
- A producer of a partitioned topic reported the state WaitingForExclusive, and not PartiallyConnected, while only some of its partitions were connected
- A producer without a ProducerName was a new producer for the broker every time it connected again, so the broker did not deduplicate the messages that it sent again after a reconnect. It now sends the name that the broker generated, the same producer id and a growing epoch when it connects again, like the Java client, so deduplication works for it too. A producer with a ProducerName is sent as before. A broker that does not check whether the old connection is still alive (for example Pulsar 3.1.0) refuses the name until it has closed the old connection, so a reconnect that the broker has not noticed can take as long as the keep alive of the broker, like it does for the Java client

## [5.3.4] - 2026-10-09

### Changed

- Updated the Google.Protobuf dependency from version 3.36.0 to 3.36.2
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.11 to 10.0.12 for .NET Standard 2.0
- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.30 to 8.0.31 for .NET 8
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.19 to 9.0.20 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.11 to 10.0.12 for .NET 10 and .NET Standard 2.X
- Updated the System.Collections.Immutable from version 10.0.11 to 10.0.12 for .NET Standard 2.X
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.11 to 10.0.12 for .NET Standard 2.X
- Updated the System.IO.Pipelines dependency from version 10.0.11 to 10.0.12 for .NET Standard 2.X
- Updated the System.Text.Json dependency from version 10.0.11 to 10.0.12 for .NET Standard 2.X

## [5.3.3] - 2026-08-26

### Changed

- Updated the Google.Protobuf dependency from version 3.35.1 to 3.36.0
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.10 to 10.0.11 for .NET Standard 2.0
- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.29 to 8.0.30 for .NET 8
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.18 to 9.0.19 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.10 to 10.0.11 for .NET 10 and .NET Standard 2.X
- Updated the System.Collections.Immutable from version 10.0.10 to 10.0.11 for .NET Standard 2.X
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.10 to 10.0.11 for .NET Standard 2.X
- Updated the System.IO.Pipelines dependency from version 10.0.10 to 10.0.11 for .NET Standard 2.X
- Updated the System.Text.Json dependency from version 10.0.10 to 10.0.11 for .NET Standard 2.X

### Fixed

- Fix reconnect races that can leave producers permanently disconnected (Thanks for the PR, Amiradel Beyg)

## [5.3.2] - 2026-08-14

### Changed

- Updated the Google.Protobuf dependency from version 3.34.1 to 3.35.1
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.7 to 10.0.10 for .NET Standard 2.0
- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.26 to 8.0.29 for .NET 8
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.15 to 9.0.18 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.7 to 10.0.10 for .NET 10 and .NET Standard 2.X
- Updated the System.Collections.Immutable from version 10.0.7 to 10.0.10 for .NET Standard 2.X
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.7 to 10.0.10 for .NET Standard 2.X
- Updated the System.IO.Pipelines dependency from version 10.0.7 to 10.0.10 for .NET Standard 2.X
- Updated the System.Text.Json dependency from version 10.0.7 to 10.0.10 for .NET Standard 2.X

### Fixed

- Race conditions causing thread pool starvation and connection leaks on broker/proxy restarts (Thanks for the PR, Eugene Polozhenkov)

## [5.3.1] - 2026-05-04

### Changed

- Updated the Google.Protobuf dependency from version 3.34.0 to 3.34.1
- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.25 to 8.0.26 for .NET 8
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.14 to 9.0.15 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.5 to 10.0.7 for .NET 10 and .NET Standard 2.X
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.5 to 10.0.7 for .NET Standard 2.0
- Updated the System.Collections.Immutable from version 10.0.5 to 10.0.7 for .NET Standard 2.0 and 2.1
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.5 to 10.0.7 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 10.0.5 to 10.0.7 for .NET Standard 2.0 and 2.1
- Updated the System.Text.Json dependency from version 10.0.5 to 10.0.7 for .NET Standard 2.0 and 2.1

## [5.3.0] - 2026-03-23

### Added

- Basic (username + password) authentication using AuthenticationFactory.Basic

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.24 to 8.0.25 for .NET 8
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.13 to 9.0.14 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.3 to 10.0.5 for .NET 10 and .NET Standard 2.X
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.3 to 10.0.5 for .NET Standard 2.0
- Updated the System.Collections.Immutable from version 10.0.3 to 10.0.5 for .NET Standard 2.0 and 2.1
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.3 to 10.0.5 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 10.0.3 to 10.0.5 for .NET Standard 2.0 and 2.1
- Calling PulsarClient.CreateProducer with MaxPendingMessages set to 0 will now throw a ConfigurationException

## [5.2.2] - 2026-03-05

### Changed

- Updated the Google.Protobuf dependency from version 3.33.5 to 3.34.0

### Fixed

- Fixed Schema.Json\<T>() generating empty schema definition

## [5.2.1] - 2026-02-26

### Fixed

- Fixed issue where a producer reconnects and could get ProducerSendReceiptOrderingException in SubProducer.ProcessReceipt

## [5.2.0] - 2026-02-16

### Added

- System.Text.Json added as a dependency for .NET Standard 2.0 and 2.1 (for use in the new JSON schema support)
- Support for JSON schema with System.Text.Json
- Support for Protobuf schema with Google.Protobuf

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.23 to 8.0.24 for .NET 8
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.12 to 9.0.13 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.2 to 10.0.3 for .NET 10 and .NET Standard 2.X
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.2 to 10.0.3 for .NET Standard 2.0
- Updated the System.Collections.Immutable from version 10.0.2 to 10.0.3 for .NET Standard 2.0 and 2.1
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.2 to 10.0.3 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 10.0.2 to 10.0.3 for .NET Standard 2.0 and 2.1

## [5.1.2] - 2026-02-02

### Changed

- Updated the Google.Protobuf dependency from version 3.33.2 to 3.33.5
- Downgraded the Microsoft.Extensions.ObjectPool dependency from version 10.0.1 to 8.0.23 for .NET 8
- Downgraded the Microsoft.Extensions.ObjectPool dependency from version 10.0.1 to 9.0.12 for .NET 9
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.1 to 10.0.2 for .NET 10 and .NET Standard 2.X
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.1 to 10.0.2 for .NET Standard 2.0
- Updated the System.Collections.Immutable from version 10.0.1 to 10.0.2 for .NET Standard 2.0 and 2.1
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.1 to 10.0.2 for .NET Standard 2.0 and 2.1
- Downgraded the System.IO.Pipelines dependency from version 10.0.1 to 8.0.0 for .NET 8
- Updated the System.IO.Pipelines dependency from version 10.0.1 to 10.0.2 for .NET Standard 2.0 and 2.1

## [5.1.1] - 2025-12-12

### Changed

- Updated the Google.Protobuf dependency from version 3.33.1 to 3.33.2
- Updated the Microsoft.Extensions.ObjectPool dependency from version 10.0.0 to 10.0.1
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 10.0.0 to 10.0.1 for .NET Standard 2.0
- Updated the System.Collections.Immutable from version 10.0.0 to 10.0.1 for .NET Standard 2.0 and 2.1
- Updated the System.Diagnostics.DiagnosticSource dependency from version 10.0.0 to 10.0.1 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 10.0.0 to 10.0.1 for .NET Standard 2.0 and 2.1 and .NET 8

## [5.1.0] - 2025-11-24

### Added

- .NET 10 added as a target framework
- Support for custom remote certificate validation

### Changed

- Updated the Google.Protobuf dependency from version 3.33.0 to 3.33.1
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.10 to 10.0.0
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.10 to 10.0.0 for .NET Standard 2.0
- Updated the System.Collections.Immutable from version 9.0.10 to 10.0.0 for .NET Standard 2.0 and 2.1
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.10 to 10.0.0 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.10 to 10.0.0 for .NET Standard 2.0 and 2.1 and .NET 8

## [5.0.0] - 2025-10-24

### Changed

- Replaced the protobuf-net (version 3.2.56) dependency with Google.Protobuf (version 3.33.0)
- System.Collections.Immutable (version 9.0.10) is now a dependency for .NET Standard 2.0 and 2.1
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.9 to 9.0.10
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.9 to 9.0.10 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.9 to 9.0.10 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.9 to 9.0.10 for .NET Standard 2.0 and 2.1 and .NET 8

### Removed

- The HashDepot dependency has been removed
- .NET 6 and 7 removed as a target frameworks

## [4.3.2] - 2025-09-17

### Changed

- Updated the protobuf-net dependency from version 3.2.52 to 3.2.56
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.7 to 9.0.9
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.7 to 9.0.9 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.7 to 9.0.9 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.7 to 9.0.9 for .NET 8 and 9

## [4.3.1] - 2025-07-16

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.5 to 9.0.7
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.5 to 9.0.7 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.5 to 9.0.7 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.5 to 9.0.7 for .NET 8 and 9

## [4.3.0] - 2025-06-10

### Added

- Support for AllowOutOfOrderDelivery on KeyShared subscriptions

### Fixed

- Issue where calling Receive in some circumstances would trigger a flow command and thereby increasing permits

## [4.2.4] - 2025-05-14

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.4 to 9.0.5
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.4 to 9.0.5 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.4 to 9.0.5 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.4 to 9.0.5 for .NET 8 and 9

### Fixed

- Memory leak when consuming from a partitioned topic

## [4.2.3] - 2025-04-23

### Changed

- Updated the protobuf-net dependency from version 3.2.46 to 3.2.52
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.3 to 9.0.4
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.3 to 9.0.4 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.3 to 9.0.4 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.3 to 9.0.4 for .NET 8 and 9

## [4.2.2] - 2025-03-17

### Changed

- The consumer no longer sends the schema to the broker when the schema type is 'None'

## [4.2.1] - 2025-03-14

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.2 to 9.0.3
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.2 to 9.0.3 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.2 to 9.0.3 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.2 to 9.0.3 for .NET 8 and 9
- The producer no longer sends the schema to the broker when the schema type is 'None'

## [4.2.0] - 2025-03-06

### Added

- Support for [Apache.Avro](https://www.nuget.org/packages/Apache.Avro) with Avro.Specific.ISpecificRecord and Avro.Generic.GenericRecord

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.1 to 9.0.2
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.1 to 9.0.2 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.1 to 9.0.2 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.1 to 9.0.2 for .NET 8 and 9

## [4.1.0] - 2025-02-05

### Added

- Support for multi-topic subscriptions given either a list of topics and/or a topics pattern

### Changed

- Updated the protobuf-net dependency from version 3.2.45 to 3.2.46

## [4.0.0] - 2025-01-27

### Added

- The subscription name and role prefix can now be set for the reader

### Changed

- **Breaking**: The consumer, reader, and producer now implements IStateHolder instead of IState
- Updated the Microsoft.Extensions.ObjectPool dependency from version 9.0.0 to 9.0.1
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 9.0.0 to 9.0.1 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 9.0.0 to 9.0.1 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 9.0.0 to 9.0.1 for .NET 8 and 9

### Removed

- Support for DotNetZip for ZLIB compression

## [3.6.0] - 2024-12-09

### Added

- Support for ZLIB compression via the [System.IO.Compression](https://learn.microsoft.com/en-us/dotnet/api/system.io.compression) API for .NET 6, 7, 8 and 9

## [3.5.0] - 2024-11-18

### Added

- .NET 9 added as a target framework

### Changed

- Updated the protobuf-net dependency from version 3.2.30 to 3.2.45
- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.10 to 9.0.0
- Updated the Microsoft.Bcl.AsyncInterfaces dependency from version 8.0.0 to 9.0.0 for .NET Standard 2.0
- Updated the Microsoft.Bcl.HashCode dependency from version 1.1.1 to 6.0.0 for .NET Standard 2.0
- Updated the System.Diagnostics.DiagnosticSource dependency from version 8.0.1 to 9.0.0 for .NET Standard 2.0 and 2.1
- Updated the System.IO.Pipelines dependency from version 8.0.0 to 9.0.0 for .NET 8 and 9

## [3.4.0] - 2024-10-25

### Added

- Multiple messages can now be acknowledged with Acknowledge(IEnumerable\<MessageId> messageIds, CancellationToken cancellationToken)
- ProcessingOptions has a new ShutdownGracePeriod property for doing a graceful shutdown by allowing active tasks to finish

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.7 to 8.0.10
- 'SslPolicyErrors' are added to the 'Data' property of the exception thrown when failing to connect

- ### Fixed

- When disposing producers, consumers, or readers 'DisposeAsync' would sometimes hang

## [3.3.2] - 2024-08-07

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.6 to 8.0.7

### Fixed

- Under some circumstances 'AuthenticateAsClientAsync' will hang after a network issue. Setting the receive and send timeouts on the TCP client should fix this

## [3.3.1] - 2024-06-24

### Added

- The consumer's subscription type is now part of the `IConsumer` interface

### Fixed

- Fixed race condition in `Producer` between `Send(...)` and `DisposeAsync()` dispose causing an unintended `DivideByZeroException`.
  It now throws a `ProducerClosedException`.
- The `Process` extension method will hang when called with EnsureOrderedAcknowledgment set to true, a shared subscription and MaxDegreeOfParallelism above 1.
  It now throws a `ProcessingException` when EnsureOrderedAcknowledgment is set to true and with a shared subscription type.

## [3.3.0] - 2024-06-10

### Added

- Producer properties can be added when creating a producer

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.4 to 8.0.6

## [3.2.1] - 2024-04-24

### Fixed

- RoundRobinPartitionRouter and SinglePartitionRouter could return a negative number. They have been changed to ensure they always return a non-negative integer.

## [3.2.0] - 2024-04-09

### Added

- IReceive now has a 'TryReceive' extension method allowing for non-blocking retrieval of buffered messages

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 8.0.1 to 8.0.3

### Fixed

- Disposing a newly created consumer or reader could result in an exception being thrown
- Concurrent 'Receive' invocations on a partitioned topic could result in an exception being thrown

## [3.1.2] - 2024-01-29

### Fixed

- When sending a message that is too large the broker will close the tcp connection, causing the producer to disconnect, reconnect, and retry in an endless loop.
  Now a TooLargeMessageException is thrown (will be given to the exception handler) and the producer's state is changed to 'Faulted'
- A race condition could cause Send(..) operations of the producer to hang after a reconnect

## [3.1.1] - 2023-12-11

### Fixed

- Fixed a bug where disposing a disconnected consumer, reader or producer would cause a hang

## [3.1.0] - 2023-11-28

### Added

- .NET 8 added as a target framework

### Changed

- Updated the Microsoft.Extensions.ObjectPool dependency from version 7.0.11 to 8.0.0
- Updated the System.IO.Pipelines dependency from version 7.0.0 to 8.0.0

### Fixed

- A race condition could cause a hang when trying to dispose a client
- A race condition could cause a producer, consumer, or reader to never exit the disconnected state

### Removed

- The 'GetLastMessageId' method on IReader and IConsumer has been removed. Use 'GetLastMessageIds' instead
- The 'AuthenticateUsingToken' method on IPulsarClientBuilder has been removed. Use Authentication(AuthenticationFactory.Token(...)) instead

## [3.0.2] - 2023-09-19

### Fixed

- When calling GetLastMessageId(s) on a Reader the topic was still added when returning MessageId.Earliest

## [3.0.1] - 2023-09-15

### Changed

- When calling GetLastMessageId(s) on a Reader or Consumer, it returns a MessageId without the topic field if MessageId.Earliest is found

### Fixed

- Fixed issue with the DotPulsar client not handling connection faults for consumers and readers

## [3.0.0] - 2023-08-30

### Added

- Added partitioned topic support for the Consumer and Reader (was already implemented for the Producer)
- The Reader and Consumer have a new 'PartiallyConnected' state (the Producer already had this state)
- The Producer has a new `Fenced` state (which is a final state)
- Support for `ProducerAccessMode` was added to prevent multiple producers on a single topic
- The ability to explicitly set compression information on an outgoing message using `MessageMetadata` (for sending pre-compressed messages)
- MessageId now includes an extra field for the topic
- A TryParse method was added to MessageId for converting a string (from ToString) to a MessageId

### Changed

- The DelayedStateMonitor extension method now invokes onStateLeft when the initial state change is to a final state

### Fixed

- Issue preventing readers from correctly going into the `Faulted` state
- Calling `await Send(...)` on a producer did not correctly terminate with an exception when a send operation failed (e.g. because the producer faulted)
- The 'Partition' in 'MessageId' will now be set to the correct partition when producing to partitioned topics
- The OnStateChangeFrom extension method with delay functionality returned the inputted state but should return the current state
- The DelayedStateMonitor extension method invoked onStateLeft with the inputted state but should have invoked it with the current state
- Consumers, Producers, and Readers will now disconnect if the server does not reply to a ping

### Deprecated

- GetLastMessageId of the Consumer and Reader is deprecated, and soon to be removed. Please use GetLastMessageIds instead.

## [2.11.1] - 2023-07-05

### Fixed

- Fixed issue with `Send` extension methods that do include `MessageMetadata` in the parameter list. The issue prevents more than two messages from being published on namespaces where deduplication is enabled.

## [2.11.0] - 2023-03-13

### Added

- 'ReplicateSubscriptionState' can now be set when creating a consumer. The default is 'false'

### Fixed

- Fixed a memory leak related to the internal locking mechanism

## [2.10.2] - 2023-02-17

### Fixed

- Fixed another memory leak introduced in 2.8.0 with internal rewrite of producer functionality

## [2.10.1] - 2023-02-15

### Fixed

- Fixed a memory leak introduced in 2.8.0 with internal rewrite of producer functionality

## [2.10.0] - 2023-01-31

### Added

- ConsumerFaultedException, ProducerFaultedException, ReaderFaultedException that all inherit from FaultedException. The inner exception will show why it faulted

### Changed

- If a consumer, reader, or producer is faulted all method calls will throw a ConsumerFaultedException, ProducerFaultedException, or ReaderFaultedException

### Fixed

- Fixed an issue introduced in version `2.8.0`, where send operations would hang after re-establishing the connection to the broker

## [2.9.0] - 2023-01-26

### Added

- When building a Pulsar client you can now specify whether the certificate revocation list is checked during authentication. The default is 'true'

## [2.8.0] - 2023-01-20

### Added

- Subscription properties can be added when creating a consumer
- Support ordered asynchronous `Send(...)` through a `SendChannel` for use cases that require very high throughput.
  The `SendChannel` is accessible through the `IProducer<TMessage>` interface and allows the user to block
  future `Send(...)` calls with `Complete()` and awaiting outstanding send operations with `await Completion()`.

## [2.7.0] - 2022-12-08

### Added

- DelayedStateMonitor for IState where onStateLeft returns a fault context that is passed to onStateReached

## [2.6.0] - 2022-12-06

### Added

- .NET 7 added as a target framework
- Delay option for OnStateChangeFrom/To for IState and StateChangedFrom/To for IConsumer, IProducer, and IReader
- DelayedStateMonitor extension methods for IState with Func and Action callbacks

### Removed

- .NET Core 3.1 removed as a target framework
- .NET 5.0 removed as a target framework

## [2.5.2] - 2022-11-02

### Changed

- A temporarily broken connection could cause DotPulsar to mark consumers, readers, and producers as disconnected while the broker was unaware of the problem. When reconnecting the broker would claim that the consumer, reader or producer is still active. Therefore the default exception handling will now retry (instead of rethrow/fault) on these two exceptions (waiting for the broker to realize that the existing connections are dead)
    - ConsumerBusyException
    - ProducerBusyException

### Fixed

- Under certain circumstances a double connection attempt could be initiated, resulting in one good connection and a looping connection calling the exception pipeline

## [2.5.1] - 2022-11-01

### Fixed

- The 'Process' extension method for IConsumer\<TMessage\> will now use the cancellation token when acknowledging messages

## [2.5.0] - 2022-10-28

### Added

- The 'Process' extension method for IConsumer\<TMessage\> can now be used for parallel processing of messages. You can control:
    - Whether ordered acknowledgment should be enforced
    - The maximum number of messages that may be processed concurrently
    - The maximum number of messages that may be processed per task
    - The TaskScheduler to use for scheduling tasks
- The trace created when sending a message can now be automatically linked to by traces created when consuming the message. All you have to do is:
    - Make sure 'messaging.trace_parent' and 'messaging.trace_state' are not already in use in the message's metadata (properties)
    - Set 'AttachTraceInfoToMessages' on ProducerOptions or IProducerBuilder when creating a producer
    - Set 'LinkTraces' on the ProcessingOptions passed to the 'Process' extension method for IConsumer\<TMessage\>

## [2.4.1] - 2022-09-16

### Changed

- Retain the original stack trace when an exception is thrown in the Producer

## [2.4.0] - 2022-06-14

### Added

- Support for [Zstd compression](https://github.com/apache/pulsar-dotpulsar/wiki/Compression) via the [ZstdSharp.Port](https://www.nuget.org/packages/ZstdSharp.Port) NuGet package

### Fixed

- The presence of [ZstdNet](https://www.nuget.org/packages/ZstdNet) on a non-Windows system will cause DotPulsar to fault consumers, producers, and readers

## [2.3.1] - 2022-04-20

### Changed

- Performance improvements, especially when consuming/reading

## [2.3.0] - 2022-03-18

### Added

- Metrics (the meter name is 'DotPulsar')
    - dotpulsar.client.count - number of active clients (gauge)
    - dotpulsar.connection.count - number of active connections (gauge)
    - dotpulsar.reader.count - number of active readers (gauge with 'topic' tag)
    - dotpulsar.consumer.count - number of active consumers (gauge with 'topic' tag)
    - dotpulsar.producer.count - number of active producers (gauge with 'topic' tag)
    - dotpulsar.producer.send.duration - Measures the duration for sending a message (histogram with 'topic' tag)
    - dotpulsar.consumer.process.duration - Measures the duration for processing a message (histogram with 'topic' and 'subscription' tags)

### Changed

- Adding a property to MessageMetadata with a key or value of null will throw an ArgumentNullException

## [2.2.0] - 2022-02-04

### Added

- Extension methods for IConsumerBuilder, IProducerBuilder, and IReaderBuilder for setting a StateChangedHandler without a cancellation token

### Changed

- The following internal exceptions are now public
    - ChannelNotReadyException (should be ignored when logging exceptions)
    - ConsumerNotFoundException (from the broker, but indicates an internal problem)
    - ServiceNotReadyException (the broker is not ready)
    - TooManyRequestsException (the broker is getting too many requests)

## [2.1.0] - 2022-02-02

### Added

- Support for on the fly authentication (AuthChallenge)
- The IAuthentication interface for implementing custom authentication
- IPulsarClientBuilder.Authentication(IAuthentication authentication) for using custom authentication
- The AuthenticationFactory with support for:
    - Token(string token)
    - Token(Func\<CancellationToken, ValueTask\<string\>\> tokenSupplier)

### Deprecated

- IPulsarClientBuilder.AuthenticateUsingToken(string token) is marked as obsolete. Use Authentication(AuthenticationFactory.Token(...)) instead

## [2.0.1] - 2021-11-12

### Changed

- Only the version number changed. Something went wrong (in the .NET 6 target) when building the NuGet package for release 2.0.0

## [2.0.0] - 2021-11-12

## Added

- .NET 6 added as a target framework
- [Tracing](https://github.com/apache/pulsar-dotpulsar/wiki/Tracing) support following the [guidelines](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md) from the [OpenTelemetry](https://opentelemetry.io/) project
    - Sending a message will create a producer trace and add tracing metadata to the message
    - The 'Process' extension method for IConsumer\<TMessage\> is no longer experimental and will create a consumer trace
    - The 'GetConversationId' extension method for IMessage has been added
    - The 'SetConversationId' and 'GetConversationId' extension methods for MessageMetadata have been added
- The client will send a 'ping' if there has been no activity on the connection for 30 seconds. This default can be changed by setting the 'KeepAliveInterval' on the IPulsarClientBuilder

### Changed

- **Breaking**: Sending a message without metadata is now an extension method and therefore no longer part of the ISend\<TMessage\> (and thereby IProducer\<TMessage\>) interface
- IMessageRouter: ChoosePartition(MessageMetadata? messageMetadata, int numberOfPartitions) -> ChoosePartition(MessageMetadata messageMetadata, int numberOfPartitions)
- The default behavior for the IOException has changed from 'Rethrow' to 'Retry'
- The default behavior for the MetadataException has changed from 'Retry' to 'Rethrow', meaning that it will fault the consumer, reader, and/or producer

## [1.1.2] - 2021-07-05

### Fixed

- The partitioned producer didn't pass metadata to the message router

## [1.1.1] - 2021-07-05

### Fixed

- The producer ignored message metadata provided by the user

## [1.1.0] - 2021-06-29

### Added

- The producer now supports partitioned topics
- The IMessageRouter interface with the RoundRobinPartitionRouter (default) and SinglePartitionRouter implementations
- The producer builder accepts a custom implementation of IMessageRouter

### Changed

- The producer state can now be 'PartiallyConnected'
- The KeyBytes property on MessageMetadata returned null if the key was set via a string. Now it will return string keys as UTF8 bytes

### Fixed

- Autogenerate a consumer and reader name when it's not explicitly set by the user

## [1.0.2] - 2021-04-30

### Fixed

- Closing a consumer or reader while the broker is streaming messages could take down the connection causing other consumers, readers, and producers of the connection to reconnect
- In some circumstances, the protocol bytes could be misread leading to wrong message parsing

## [1.0.1] - 2021-03-30

### Fixed

- Creating a producer with a schema for a non-existing topic failed

## [1.0.0] - 2021-03-17

### Added

- A number of resilience, correctness, and performance improvements
- The optional listener name can be set via the PulsarClientBuilder
- *Experimental*: Added an extension method for IConsumer that will 'Process' and auto-acknowledge messages while creating an Activity (useful for doing tracing)
- Schema support for the following types
    - Boolean
    - Bytes (using byte[] and ReadOnlySequence\<byte\>)
    - String (UTF-8, UTF-16, and US-ASCII)
    - INT8, INT16, INT32, and INT64
    - Float and Double
    - Time (using TimeSpan)
    - Timestamp and Date (using DateTime)

### Changed

- **Breaking**: Building a producer will now create an IProducer\<T\>\
  The non-generic IProducer interface is still there, but messages can only be sent (ISend) with IProducer\<T\>
- **Breaking**: Building a reader or consumer will now create an IConsumer\<T\> or IReader\<T\>\
  The non-generic IReader and IConsumer are still there, but messages can only be consumed/read (IReceive) with IConsumer\<T\> and IReader\<T\>
- **Breaking**: Receiving a message with now return an IMessage\<T\> instead of the Message class (which is now internal)\
  The non-generic IMessage can be used if 'Value()' (decoding the 'Data' bytes) is not used (when just handling raw messages)
- **Breaking**: The message builder is now generic
- Setting an Action and Func StateChangedHandler on the ConsumerBuilder, ReaderBuilder, and ProducerBuilder are now extension methods
- Setting an Action and Func ExceptionHandler on the PulsarClientBuilder are now extension methods

### Fixed

- When the broker sends a CommandClose[Producer/Consumer] all in-flight (and following) requests to the broker are ignored.\
  Even though we reconnect the consumer, reader, or producer the tasks for the in-flight requests will hang as long as the connection is kept alive by other producers/consumers/readers.\
  This situation is now handled and the requests will be sent again on the new connection.

## [0.11.0] - 2021-02-21

### Added

- The Consumer and Reader now share the IReceive interface for receiving a single message: ValueTask\<Message\> Receive(CancellationToken cancellationToken)
- The Consumer and Reader now share the ISeek interface for seeking on message-id and publish time
- The Consumer and Reader now share the IGetLastMessageId interface for getting the last message-id on a topic
- The Consumer, Reader, and Producer now share the IState interface adding 'OnStateChangeFrom' and 'OnStateChangeTo'
- The PulsarClient, Consumer, Reader, and Producer now have the read-only property 'ServiceUrl'
- The Consumer now have the read-only property 'SubscriptionName'
- All message compression types are now supported (listed below). Please read this [compression how-to](https://github.com/apache/pulsar-dotpulsar/wiki/Compression)
    - LZ4
    - SNAPPY
    - ZLIB
    - ZSTD

### Changed

- MessageId.ToString() now returns a string matching that of other clients: "{LedgerId}:{EntryId}:{Partition}:{BatchIndex}"
- A lot of methods were made into extension methods and now require a using statement for 'DotPulsar.Extensions'
    - Producer.StateChangedTo(ProducerState state, CancellationToken cancellationToken)
    - Producer.StateChangedFrom(ProducerState state, CancellationToken cancellationToken)
    - Producer.Send(byte[] data, CancellationToken cancellationToken)
    - Producer.Send(ReadOnlyMemory\<byte\> data, CancellationToken cancellationToken)
    - Producer.Send(MessageMetadata metadata, byte[] data, CancellationToken cancellationToken)
    - Producer.Send(MessageMetadata metadata, ReadOnlyMemory\<byte\> data, CancellationToken cancellationToken)
    - Consumer.Acknowledge(Message message, CancellationToken cancellationToken)
    - Consumer.AcknowledgeCumulative(Message message, CancellationToken cancellationToken)
    - Consumer.StateChangedTo(ConsumerState state, CancellationToken cancellationToken)
    - Consumer.StateChangedFrom(ConsumerState state, CancellationToken cancellationToken)
    - Consumer.Messages(CancellationToken cancellationToken)
    - Consumer.Seek(DateTime publishTime, CancellationToken cancellationToken)
    - Consumer.Seek(DateTimeOffset publishTime, CancellationToken cancellationToken)
    - Reader.StateChangedTo(ReaderState state, CancellationToken cancellationToken)
    - Reader.StateChangedFrom(ReaderState state, CancellationToken cancellationToken)
    - Reader.Messages(CancellationToken cancellationToken)
    - Reader.Seek(DateTime publishTime, CancellationToken cancellationToken)
    - Reader.Seek(DateTimeOffset publishTime, CancellationToken cancellationToken)

### Fixed

- Before the Consumer and Reader would throw an ArgumentOutOfRangeException if they encountered a compressed message. Now they will throw a CompressionException if the compression type is not supported
- DotPulsarEventSource (performance counters) was only enabled for .NET Standard 2.1. Now it's enabled for all target frameworks except .NET Standard 2.0

## [0.10.1] - 2020-12-23

### Added

- MessageId implements IComparable\<MessageId\>

### Fixed

- Do not throw exceptions when disposing consumers, readers, or producers

## [0.10.0] - 2020-12-16

### Added

- Added performance improvements when receiving data
- Added the IHandleStateChanged\<TStateChanged\> interface for easier state monitoring
- Added StateChangedHandler methods on ConsumerBuilder, ReaderBuilder, and ProducerBuilder for easier state monitoring
- Added StateChangedHandler property on ConsumerOptions, ReaderOptions, and ProducerOptions for easier state monitoring
- Added four new DotPulsarExceptions: InvalidTransactionStatusException, NotAllowedException, TransactionConflictException and TransactionCoordinatorNotFoundException
- Added properties on Message to read EventTime and PublishTime as DateTime
- Added methods on the IMessageBuilder to set EventTime and DeliverAt using DateTime
- Added properties on MessageMetadata to set EventTime and DeliverAtTime using DateTime
- Added seeking by MessageId on the Reader
- Added seeking by message publish time on the Consumer and Reader
- Added GetLastMessageId on the Reader

### Changed

- The protobuf-net dependency is upgraded from 2.4.6 to 3.0.73 to get support for ReadOnlySequence\<byte\>

### Fixed

- Reconnection issues when seeking

## [0.9.7] - 2020-12-04

### Added

- Added an ExceptionHandler method on the IPulsarClientBuilder taking an Action\<ExceptionContext\> for easy sync exception handling
- Added .NET 5 as a target framework and started using C# 9.0
- Added performance improvements when sending and receiving data
- The default values for ConsumerOptions, ReaderOptions, and ProducerOptions are now public

## [0.9.6] - 2020-10-15

### Fixed

- Fixed missing metadata properties in batched messages containing only one message
- Fixed potential torn reads in EventCounters
