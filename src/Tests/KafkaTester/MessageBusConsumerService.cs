using Foundatio.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTester
{
    public class MessageBusConsumeService : IHostedService
    {

        private readonly IMessageBus _messageBus;     
        private readonly ILogger<MessageBusConsumeService> _logger;

        public MessageBusConsumeService(IMessageBus messageBus,          
            ILogger<MessageBusConsumeService> logger)
        {
            this._messageBus = messageBus;          
            this._logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {

            this._messageBus.SubscribeAsync<KafkaMessage>(Consume, cancellationToken);

            this._logger.LogInformation("MessageBusConsumeService is Started!");
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            this._messageBus.Dispose();
            this._logger.LogInformation("MessageBusConsumeService is Stop!");
            return Task.CompletedTask;
        }

        private void Consume(KafkaMessage item)
        {
            this._logger.LogInformation("messageId={messageId},content = {data}", item.MessageId, item.Content);
        }
    }
}