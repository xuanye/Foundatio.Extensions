using Foundatio.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTester
{
    public class MessageBusConsumerService : IHostedService
    {
        private IMessageBus _MessageBus;
        private readonly IServiceProvider _serviceProvider;

        public MessageBusConsumerService(IServiceProvider serviceProvider)
        {
            // Since creation == initialization/connection, we delay retrieving the service
            // until we're asked to start.
            _serviceProvider = serviceProvider;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _MessageBus = _serviceProvider.GetRequiredService<IMessageBus>();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _MessageBus?.Dispose();
            return Task.CompletedTask;
        }
    }
}