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
using Microsoft.Extensions.DependencyInjection;

using NServiceBus.Features;

namespace PPWCode.NServiceBus.Host.I.EntityFrameworkCore;

/// <summary>
///     This NServiceBus feature implements a unit of work behavior for message
///     processing.  The behavior wraps the message handling inside a database
///     transaction linked to an Entity Framework Core <see cref="DbContext"/>.
/// </summary>
public class SqlTransactionUnitOfWork : Feature
{
    /// <inheritdoc />
    protected override void Setup(FeatureConfigurationContext context)
        => context.Pipeline.Register(
            nameof(SqlTransactionUnitOfWorkBehavior),
            serviceProvider => ActivatorUtilities.CreateInstance<SqlTransactionUnitOfWorkBehavior>(serviceProvider, new SqlTransactionUnitOfWorkOptions()),
            "Make sure that we have a db-context with an explicit transaction while handling the message.");
}
