/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar;

using DotPulsar.Abstractions;

/// <summary>
/// The producer building options.
/// </summary>
public sealed class ProducerOptions<TMessage>
{
    /// <summary>
    /// The default compression type.
    /// </summary>
    public static readonly CompressionType DefaultCompressionType = CompressionType.None;

    /// <summary>
    /// The default initial sequence id.
    /// </summary>
    public static readonly ulong DefaultInitialSequenceId = 0;

    /// <summary>
    /// The default producer access mode.
    /// </summary>
    public static readonly ProducerAccessMode DefaultProducerAccessMode = ProducerAccessMode.Shared;

    /// <summary>
    /// The default batcher type.
    /// </summary>
    public static readonly BatcherType DefaultBatcherType = BatcherType.Default;

    /// <summary>
    /// The default maximum number of bytes in a batch.
    /// </summary>
    public static readonly uint DefaultBatchingMaxBytes = 128 * 1024;

    /// <summary>
    /// The default maximum number of messages in a batch.
    /// </summary>
    public static readonly uint DefaultBatchingMaxMessages = 1000;

    /// <summary>
    /// The default maximum delay before a batch is sent.
    /// </summary>
    public static readonly TimeSpan DefaultBatchingMaxPublishDelay = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// The default of whether batching is enabled.
    /// </summary>
    public static readonly bool DefaultEnableBatching = false;

    /// <summary>
    /// The default send timeout.
    /// </summary>
    public static readonly TimeSpan DefaultSendTimeout = TimeSpan.Zero;

    /// <summary>
    /// Initializes a new instance using the specified topic.
    /// </summary>
    public ProducerOptions(string topic, ISchema<TMessage> schema)
    {
        AttachTraceInfoToMessages = false;
        CompressionType = DefaultCompressionType;
        InitialSequenceId = DefaultInitialSequenceId;
        ProducerAccessMode = DefaultProducerAccessMode;
        Topic = topic;
        Schema = schema;
        MessageRouter = new RoundRobinPartitionRouter();
        ProducerProperties = [];
        BatcherType = DefaultBatcherType;
        BatchingMaxBytes = DefaultBatchingMaxBytes;
        BatchingMaxMessages = DefaultBatchingMaxMessages;
        BatchingMaxPublishDelay = DefaultBatchingMaxPublishDelay;
        EnableBatching = DefaultEnableBatching;
        SendTimeout = DefaultSendTimeout;
    }

    /// <summary>
    /// Whether to attach the sending trace's parent and state to the outgoing messages metadata. The default is 'false'.
    /// </summary>
    public bool AttachTraceInfoToMessages { get; set; }

    /// <summary>
    /// Set the compression type. The default is 'None'.
    /// </summary>
    public CompressionType CompressionType { get; set; }

    /// <summary>
    /// Set the initial sequence id. The default is 0.
    /// </summary>
    public ulong InitialSequenceId { get; set; }

    /// <summary>
    /// Set the producer access mode. The default is 'Shared'
    /// </summary>
    public ProducerAccessMode ProducerAccessMode { get; set; }

    /// <summary>
    /// Set the producer name. This is optional.
    /// </summary>
    public string? ProducerName { get; set; }

    /// <summary>
    /// Set the schema. This is required.
    /// </summary>
    public ISchema<TMessage> Schema { get; set; }

    /// <summary>
    /// Register a state changed handler. This is optional.
    /// </summary>
    public IHandleStateChanged<ProducerStateChanged>? StateChangedHandler { get; set; }

    /// <summary>
    /// Set the topic for this producer. This is required.
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// Set the message router. The default router is the Round Robin partition router.
    /// </summary>
    public IMessageRouter MessageRouter { get; set; }

    /// <summary>
    /// Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
    /// </summary>
    public uint MaxPendingMessages { get; set; }

    /// <summary>
    /// Add/Set the producers's properties. This is optional.
    /// </summary>
    public Dictionary<string, string> ProducerProperties { get; set; }

    // The name of the subscription that the broker creates on the topic when the producer is created. It is used by the dead letter producer of a consumer.
    internal string? InitialSubscriptionName { get; set; }

    /// <summary>
    /// Set the batcher type. The default is 'Default'.
    /// </summary>
    public BatcherType BatcherType { get; set; }

    /// <summary>
    /// Set the maximum number of bytes (uncompressed) in a batch. 0 means that the maximum message size of the broker is used. The default is 131072 (128 KB).
    /// </summary>
    public uint BatchingMaxBytes { get; set; }

    /// <summary>
    /// Set the maximum number of messages in a batch. 0 means that there is no limit. The default is 1000.
    /// </summary>
    public uint BatchingMaxMessages { get; set; }

    /// <summary>
    /// Set the maximum delay between the first message being added to a batch and the batch being sent. It must be at least 1 millisecond. The default is 1 millisecond.
    /// </summary>
    public TimeSpan BatchingMaxPublishDelay { get; set; }

    /// <summary>
    /// Whether to batch messages. The default is 'false'.
    /// </summary>
    public bool EnableBatching { get; set; }

    /// <summary>
    /// Set the send timeout. If the broker has not acknowledged the oldest message that is waiting within this time, every message that is waiting in the producer (of the partition) fails with an OperationTimeoutException. TimeSpan.Zero disables the timeout. The default is 'TimeSpan.Zero'.
    /// </summary>
    public TimeSpan SendTimeout { get; set; }
}
