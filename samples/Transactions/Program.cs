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

using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;

// Consumes messages from one topic and produces two messages per consumed message, to two other topics.
// The acknowledgment of the consumed message and the two produced messages are one atomic operation:
// either all of them happen (the transaction is committed) or none of them do (the transaction is aborted).

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (sender, args) =>
{
    cts.Cancel();
    args.Cancel = true;
};

// Transactions must be enabled on the client. The connection to the transaction coordinator is created when the first transaction is created.
await using var client = PulsarClient
    .Builder()
    .EnableTransaction(true)
    .ExceptionHandler(ExceptionHandler)
    .Build(); // Connecting to pulsar://localhost:6650

await using var consumer = client.NewConsumer(Schema.String)
    .StateChangedHandler(ConsumerStateChangedHandler)
    .SubscriptionName("transactional-sub")
    .SubscriptionType(SubscriptionType.Exclusive)
    .InitialPosition(SubscriptionInitialPosition.Earliest)
    .Topic("persistent://public/default/input-topic")
    .Create();

await using var producer1 = client.NewProducer(Schema.String)
    .StateChangedHandler(ProducerStateChangedHandler)
    .Topic("persistent://public/default/output-topic-1")
    .Create();

await using var producer2 = client.NewProducer(Schema.String)
    .StateChangedHandler(ProducerStateChangedHandler)
    .Topic("persistent://public/default/output-topic-2")
    .Create();

Console.WriteLine("Press Ctrl+C to exit");

await ProcessMessages(cts.Token);

async Task ProcessMessages(CancellationToken cancellationToken)
{
    try
    {
        await foreach (var message in consumer.Messages(cancellationToken))
        {
            var outputMessage1 = message.Value() + "-output-1";
            var outputMessage2 = message.Value() + "-output-2";

            var transaction = await client
                .NewTransaction()
                .Timeout(TimeSpan.FromMinutes(1))
                .Create(cancellationToken);

            try
            {
                // The messages are sent and the acknowledgment is made as part of the transaction.
                // None of them are visible to other consumers, and the consumed message stays unacknowledged, until the transaction is committed.
                var send1 = producer1.NewMessage().Transaction(transaction).Send(outputMessage1, cancellationToken);
                var send2 = producer2.NewMessage().Transaction(transaction).Send(outputMessage2, cancellationToken);
                var acknowledge = consumer.Acknowledge(message.MessageId, transaction, cancellationToken);

                // Waits for the sends and the acknowledgment to complete and then commits the transaction.
                // If any of them have failed, the transaction is aborted and an exception is thrown.
                await transaction.Commit(cancellationToken);

                var messageId1 = await send1;
                var messageId2 = await send2;
                await acknowledge;

                Console.WriteLine($"Processed '{message.Value()}'. Sent '{outputMessage1}' ({messageId1}) and '{outputMessage2}' ({messageId2})");
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                Console.WriteLine($"Could not process '{message.Value()}', the transaction is aborted: {exception.Message}");
                await transaction.Abort(CancellationToken.None);
            }
        }
    }
    catch (OperationCanceledException) { }
}

void ExceptionHandler(ExceptionContext context) =>
    Console.WriteLine($"The PulsarClient got an exception: {context.Exception}");

void ConsumerStateChangedHandler(ConsumerStateChanged stateChanged) =>
    Console.WriteLine($"The consumer for topic '{stateChanged.Consumer.Topic}' changed state to '{stateChanged.ConsumerState}'");

void ProducerStateChangedHandler(ProducerStateChanged stateChanged) =>
    Console.WriteLine($"The producer for topic '{stateChanged.Producer.Topic}' changed state to '{stateChanged.ProducerState}'");
