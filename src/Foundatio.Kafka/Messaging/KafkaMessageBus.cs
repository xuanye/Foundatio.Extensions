using Confluent.Kafka;
using Foundatio.AsyncEx;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.Messaging
{
    public class KafkaMessageBus : MessageBusBase<KafkaMessageBusOptions>
    {
        private readonly Producer _producer;
        private readonly Consumer _consumer;

        private readonly AsyncLock _lock = new AsyncLock();

        private bool _isSubscribed;
        private bool _stopPoll;

        private readonly ILogger<KafkaMessageBus> _mylogger;
        public KafkaMessageBus(KafkaMessageBusOptions options,ILoggerFactory loggerFactory) : base(options)
        {

            this._mylogger = loggerFactory.CreateLogger<KafkaMessageBus>();
            var realConf = new Dictionary<string, object>();
            if (string.IsNullOrEmpty(options.BootStrapServers))
            {
                throw new ArgumentNullException("bootstrap.servers");
            }
            realConf.Add("bootstrap.servers", options.BootStrapServers);
            if (!string.IsNullOrEmpty(options.GroupId))
            {
                realConf.Add("group.id", options.GroupId);
            }
            if (options.AutoCommitIntervalMS > 0)
            {
                realConf.Add("auto.commit.interval.ms", options.AutoCommitIntervalMS);
            }
            if (!string.IsNullOrEmpty(options.AutoOffSetReset))
            {
                realConf.Add("auto.offset.reset", options.AutoOffSetReset);
            }
            if (options.Arguments != null)
            {
                foreach (var kv in options.Arguments)
                {
                    if (!realConf.ContainsKey(kv.Key))
                    {
                        realConf.Add(kv.Key, kv.Value);
                    }
                }
            }
            if ((options.ClientMode & ClientMode.Producer) == ClientMode.Producer)
            {            
                this._producer = new Producer(realConf);
            }
            if ((options.ClientMode & ClientMode.Consumer) == ClientMode.Consumer)
            {
                this._consumer = new Consumer(realConf);
            }
        }

        public KafkaMessageBus(Builder<KafkaMessageBusOptionsBuilder, KafkaMessageBusOptions> config,ILoggerFactory loggerFactory)
           : this(config(new KafkaMessageBusOptionsBuilder()).Build(),loggerFactory)
        {
        }

        protected override async Task EnsureTopicSubscriptionAsync(CancellationToken cancellationToken)
        {
            if (this._isSubscribed)
                return;

            using (await this._lock.LockAsync().AnyContext())
            {
                if (this._isSubscribed)
                    return;

                bool isTraceLogLevelEnabled = this._logger.IsEnabled(LogLevel.Trace);
                
                if (isTraceLogLevelEnabled) this._logger.LogTrace("Subscribing to topic: {Topic}", _options.Topic);

                this._consumer.OnMessage += OnMessage;

                this._consumer.OnError += OnError;

                this._consumer.OnConsumeError += OnConsumeError;

                this._consumer.Subscribe(this._options.Topic);

                this._isSubscribed = true;

                StartPoll();
            }
        }

        private void OnConsumeError(object sender, Message msg)
        {
            this._mylogger.LogError("OnConsumeError, Message ={msg}", msg);
           
        }

        private void OnError(object sender, Error e)
        {
            this._mylogger.LogError("OnError, ErrorMessage ={msg}", e.Reason);
        }

        private void OnMessage(object sender, Message msg)
        {
            if (this._subscribers.IsEmpty)
                return;

            MessageBusData message;
            try
            {
                message = this._serializer.Deserialize<MessageBusData>(msg.Value);
            }
            catch (Exception ex)
            {
                this._logger.LogWarning(ex, "OnMessage({KafkaMessage}) Error deserializing message: {Error}", msg, ex.Message);
                return;
            }

            SendMessageToSubscribers(message, this._serializer);
        }

        private void StartPoll()
        {
            Task.Run(() =>
           {
               while (!this._stopPoll)
               {
                   this._consumer.Poll(TimeSpan.FromMilliseconds(100));
               }
           }).AnyContext();
        }

        protected async override Task PublishImplAsync(string messageType, object message, TimeSpan? delay, CancellationToken cancellationToken)
        {
            var mappedType = GetMappedMessageType(messageType);

            if (delay.HasValue && delay.Value > TimeSpan.Zero)
            {
                this._logger.LogTrace("Schedule delayed message: {MessageType} ({Delay}ms)", messageType, delay.Value.TotalMilliseconds);

                await AddDelayedMessageAsync(mappedType, message, delay.Value).AnyContext();

                return;
            }

            this._logger.LogTrace("Message Publish: {MessageType}", messageType);

            byte[] data = this._serializer.SerializeToBytes(new MessageBusData
            {
                Type = messageType,
                Data = this._serializer.SerializeToBytes(message)
            });
           

            await this._producer.ProduceAsync(this._options.Topic, null, data).AnyContext();
        }

        public override void Dispose()
        {
            base.Dispose();
            this._stopPoll = true;

            this._producer?.Dispose();
            this._consumer?.Dispose();

            this._isSubscribed = false;
        }
      
    }
}