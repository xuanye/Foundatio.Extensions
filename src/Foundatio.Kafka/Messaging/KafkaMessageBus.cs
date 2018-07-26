using Confluent.Kafka;
using Foundatio.AsyncEx;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
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

        public KafkaMessageBus(KafkaMessageBusOptions options) : base(options)
        {
            var realConf = new Dictionary<string, Object>();
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
                _producer = new Producer(realConf);
            }
            if ((options.ClientMode & ClientMode.Consumer) == ClientMode.Consumer)
            {
                _consumer = new Consumer(realConf);
            }
        }

        public KafkaMessageBus(Builder<KafkaMessageBusOptionsBuilder, KafkaMessageBusOptions> config)
           : this(config(new KafkaMessageBusOptionsBuilder()).Build())
        {
        }

        protected override async Task EnsureTopicSubscriptionAsync(CancellationToken cancellationToken)
        {
            if (_isSubscribed)
                return;

            using (await _lock.LockAsync().AnyContext())
            {
                if (_isSubscribed)
                    return;

                bool isTraceLogLevelEnabled = _logger.IsEnabled(LogLevel.Trace);
                if (isTraceLogLevelEnabled) _logger.LogTrace("Subscribing to topic: {Topic}", _options.Topic);

                _consumer.OnMessage += OnMessage;

                _consumer.OnError += OnError;

                _consumer.OnConsumeError += OnConsumeError;

                _consumer.Subscribe(_options.Topic);

                _isSubscribed = true;

                StartPoll();
            }
        }

        private void OnConsumeError(object sender, Message msg)
        {
            _logger.LogWarning("OnConsumeError, Message ={msg}", msg);
            Console.WriteLine("OnConsumeError, Message ={msg}", msg);
        }

        private void OnError(object sender, Error e)
        {
            _logger.LogWarning("OnError, ErorrMessage ={msg}", e.Reason);
            Console.WriteLine("OnError, ErorrMessage ={0}", e.Reason);
        }

        private void OnMessage(object sender, Message msg)
        {
            if (_subscribers.IsEmpty)
                return;

            MessageBusData message;
            try
            {
                message = _serializer.Deserialize<MessageBusData>(msg.Value);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "OnMessage({KafkaMessage}) Error deserializing messsage: {Error}", msg, ex.Message);
                return;
            }

            SendMessageToSubscribers(message, _serializer);
        }

        private void StartPoll()
        {
            Task.Run(() =>
           {
               while (!_stopPoll)
               {
                   _consumer.Poll(TimeSpan.FromMilliseconds(100));
               }
           }).AnyContext();
        }

        protected async override Task PublishImplAsync(Type messageType, object message, TimeSpan? delay, CancellationToken cancellationToken)
        {
            if (delay.HasValue && delay.Value > TimeSpan.Zero)
            {
                _logger.LogTrace("Schedule delayed message: {MessageType} ({Delay}ms)", messageType.FullName, delay.Value.TotalMilliseconds);

                await AddDelayedMessageAsync(messageType, message, delay.Value).AnyContext();

                return;
            }

            _logger.LogTrace("Message Publish: {MessageType}", messageType.FullName);

            var data = _serializer.SerializeToBytes(new MessageBusData
            {
                Type = String.Concat(messageType.FullName, ", ", messageType.Assembly.GetName().Name),
                Data = _serializer.SerializeToBytes(message)
            });

            await _producer.ProduceAsync(_options.Topic, null, data).AnyContext();
        }

        public override void Dispose()
        {
            base.Dispose();
            _stopPoll = true;

            if (_producer != null)
            {
                _producer.Dispose();
            }
            if (_consumer != null)
            {
                _consumer.Dispose();
            }
            _isSubscribed = false;
        }
    }
}