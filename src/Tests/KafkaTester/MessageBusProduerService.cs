using Foundatio.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaTester
{
    public class MessageBusProduerService : BackgroundService
    {
        private readonly IMessageBus _messageBus;
        private readonly ILogger<MessageBusConsumeService> _logger;
        public MessageBusProduerService(IMessageBus messageBus,
            ILogger<MessageBusConsumeService> logger)
        {
            this._messageBus = messageBus;
            this._logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            this._logger.LogInformation("MessageBusProduerService Start!");
            int index = 1;
            while (!stoppingToken.IsCancellationRequested)
            {
                stoppingToken.ThrowIfCancellationRequested();

               
                string messageId = Guid.NewGuid().ToString("N");

                this._logger.LogInformation("MessageBusProduerService produce message {0}", messageId);
                await this._messageBus.PublishAsync(
                    new KafkaMessage() {
                        MessageId = messageId,
                        Content = $"测试消息:{index++}"
                    }
                );
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            this._messageBus.Dispose();
            this._logger.LogInformation("MessageBusProduerService Stop!");
           
        }
    }
}
