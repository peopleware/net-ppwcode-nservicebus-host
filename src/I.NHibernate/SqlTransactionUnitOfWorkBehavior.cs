// Copyright 2026 by PeopleWare n.v..
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NHibernate;

using NServiceBus.Pipeline;

using PPWCode.Vernacular.Exceptions.V;
using PPWCode.Vernacular.NHibernate.IV;

namespace PPWCode.NServiceBus.Host.I.NHibernate;

/// <summary>
///     An NServiceBus unit of work behavior that manages a database transaction for each incoming message.
///     This relies on NHibernate to provide a database <see cref="ISession"/> and transaction management.
/// </summary>
public class SqlTransactionUnitOfWorkBehavior
    : IBehavior<IIncomingPhysicalMessageContext, IIncomingPhysicalMessageContext>
{
    private readonly ILogger<SqlTransactionUnitOfWorkBehavior> _logger;
    private readonly SqlTransactionUnitOfWorkOptions _options;

    public SqlTransactionUnitOfWorkBehavior(
        ILogger<SqlTransactionUnitOfWorkBehavior> logger,
        SqlTransactionUnitOfWorkOptions options)
    {
        _logger = logger;
        _options = options;
    }

    /// <inheritdoc />
    public async Task Invoke(IIncomingPhysicalMessageContext context, Func<IIncomingPhysicalMessageContext, Task> next)
    {
        ISessionProviderAsync sessionProvider = context.Builder.GetRequiredService<ISessionProviderAsync>();
        ISession session = sessionProvider.Session;
        if (!session.IsOpen)
        {
            throw new ProgrammingError("Current session is not opened.");
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                "Start transaction with isolation level {IsolationLevel} for message {MessageId}",
                _options.IsolationLevel,
                context.MessageId);
        }

        ITransaction transaction = session.BeginTransaction(_options.IsolationLevel);

        try
        {
            await next(context).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Flushing changes to database, for message {MessageId}", context.MessageId);
            }

            await sessionProvider.FlushAsync(context.CancellationToken).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Commit changes to database, for message {MessageId}", context.MessageId);
            }

            await sessionProvider
                .SafeEnvironmentProviderAsync
                .RunAsync(
                    nameof(ITransaction.CommitAsync),
                    transaction.CommitAsync,
                    context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            _logger.LogError("Error occurred during transaction, rolling back for message {MessageId}", context.MessageId);
            await transaction.RollbackAsync(context.CancellationToken).ConfigureAwait(false);
            throw;
        }
    }
}
