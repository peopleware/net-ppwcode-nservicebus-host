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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NServiceBus.Pipeline;

namespace PPWCode.NServiceBus.Host.I.EntityFrameworkCore;

/// <summary>
///     An NServiceBus unit of work behavior that manages a database transaction for each incoming message.
///     This relies on Entity Framework Core to provide the database context and transaction management.
/// </summary>
/// <remarks>
///     This class assumes that the project-specific database context can be resolved as <see cref="DbContext"/>.
/// </remarks>
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
        await using DbContext dbContext = context.Builder.GetRequiredService<DbContext>();

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                "Start transaction with isolation level {IsolationLevel} for message {MessageId}",
                _options.IsolationLevel,
                context.MessageId);
        }

        await using IDbContextTransaction transaction =
            await dbContext.Database
                .BeginTransactionAsync(_options.IsolationLevel, context.CancellationToken)
                .ConfigureAwait(false);

        try
        {
            await next(context).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Saving changes to database, for message {MessageId}", context.MessageId);
            }

            await dbContext.SaveChangesAsync(context.CancellationToken).ConfigureAwait(false);

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Commit changes to database, for message {MessageId}", context.MessageId);
            }

            await transaction
                .CommitAsync(context.CancellationToken)
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
