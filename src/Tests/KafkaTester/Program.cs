using System;
using Foundatio.Messaging;

namespace KafkaTester
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            IMessageBus producer = new KafkaMessageBus(new KafkaMessageBusOptions()
            {
                BootStrapServers = "127.0.0.1:9092",
                ClientMode = ClientMode.Producer,
                GroupId = "TESTCLIENT",
                Topic = "TESTTOPIC"
            });

            IMessageBus consumer = new KafkaMessageBus(new KafkaMessageBusOptions()
            {
                BootStrapServers = "127.0.0.1:9092",
                ClientMode = ClientMode.Consumer,
                AutoCommitIntervalMS = 5000,
                AutoOffSetReset = "earliest",
                GroupId = "TESTCLIENT",
                Topic = "TESTTOPIC"
            });

            //consumer.SubscribeAsync<KafkaMessage>(msg => Console.WriteLine("{0},{1}", msg.MessageId, msg.Content));
            Console.WriteLine("consumer ready");
            while (true)
            {
                Console.WriteLine("请输入你需要发送的消息:");
                var content = Console.ReadLine();
                if (content == "quit")
                {
                    break;
                }
                producer.PublishAsync(new KafkaMessage()
                {
                    MessageId = Guid.NewGuid().ToString("N"),
                    Content = content
                });
            }
            Console.WriteLine("Hello World!");
        }
    }

    public class KafkaMessage
    {
        public string MessageId { get; set; }

        public string Content { get; set; }
    }
}