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

namespace DotPulsar.Tests.Internal.Transactions;

using DotPulsar.Internal.PulsarApi;

public static class TransactionResponses
{
    public static BaseCommand TcClientConnect()
        => new() { Type = BaseCommand.Types.Type.TcClientConnectResponse, TcClientConnectResponse = new CommandTcClientConnectResponse() };

    public static BaseCommand TcClientConnect(ServerError error, string message)
        => new() { Type = BaseCommand.Types.Type.TcClientConnectResponse, TcClientConnectResponse = new CommandTcClientConnectResponse { Error = error, Message = message } };

    public static BaseCommand NewTxn(ulong mostSigBits, ulong leastSigBits)
        => new() { Type = BaseCommand.Types.Type.NewTxnResponse, NewTxnResponse = new CommandNewTxnResponse { TxnidMostBits = mostSigBits, TxnidLeastBits = leastSigBits } };

    public static BaseCommand NewTxn(ServerError error, string message)
        => new() { Type = BaseCommand.Types.Type.NewTxnResponse, NewTxnResponse = new CommandNewTxnResponse { Error = error, Message = message } };

    public static BaseCommand AddPartitionToTxn()
        => new() { Type = BaseCommand.Types.Type.AddPartitionToTxnResponse, AddPartitionToTxnResponse = new CommandAddPartitionToTxnResponse() };

    public static BaseCommand AddPartitionToTxn(ServerError error, string message)
        => new() { Type = BaseCommand.Types.Type.AddPartitionToTxnResponse, AddPartitionToTxnResponse = new CommandAddPartitionToTxnResponse { Error = error, Message = message } };

    public static BaseCommand AddSubscriptionToTxn()
        => new() { Type = BaseCommand.Types.Type.AddSubscriptionToTxnResponse, AddSubscriptionToTxnResponse = new CommandAddSubscriptionToTxnResponse() };

    public static BaseCommand AddSubscriptionToTxn(ServerError error, string message)
        => new() { Type = BaseCommand.Types.Type.AddSubscriptionToTxnResponse, AddSubscriptionToTxnResponse = new CommandAddSubscriptionToTxnResponse { Error = error, Message = message } };

    public static BaseCommand EndTxn()
        => new() { Type = BaseCommand.Types.Type.EndTxnResponse, EndTxnResponse = new CommandEndTxnResponse() };

    public static BaseCommand EndTxn(ServerError error, string message)
        => new() { Type = BaseCommand.Types.Type.EndTxnResponse, EndTxnResponse = new CommandEndTxnResponse { Error = error, Message = message } };

    public static BaseCommand Error(ServerError error, string message)
        => new() { Type = BaseCommand.Types.Type.Error, Error = new CommandError { Error = error, Message = message } };

    // The successful response to a command
    public static BaseCommand Success(object command)
        => command switch
        {
            CommandTcClientConnectRequest => TcClientConnect(),
            CommandNewTxn => NewTxn(0, 0),
            CommandAddPartitionToTxn => AddPartitionToTxn(),
            CommandAddSubscriptionToTxn => AddSubscriptionToTxn(),
            CommandEndTxn => EndTxn(),
            _ => throw new ArgumentException($"There is no response for '{command.GetType().Name}'", nameof(command))
        };

    // The response with an error to a command
    public static BaseCommand Failure(object command, ServerError error, string message)
        => command switch
        {
            CommandTcClientConnectRequest => TcClientConnect(error, message),
            CommandNewTxn => NewTxn(error, message),
            CommandAddPartitionToTxn => AddPartitionToTxn(error, message),
            CommandAddSubscriptionToTxn => AddSubscriptionToTxn(error, message),
            CommandEndTxn => EndTxn(error, message),
            _ => throw new ArgumentException($"There is no response for '{command.GetType().Name}'", nameof(command))
        };
}
