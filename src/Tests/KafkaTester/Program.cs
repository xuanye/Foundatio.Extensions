using Foundatio.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace KafkaTester
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //已经在项目文件中设置了
            //System.Threading.ThreadPool.SetMinThreads(100, 100);

            //任务系统出错的情况
            TaskScheduler.UnobservedTaskException += TaskScheduler_UnobservedTaskException;

            // 创建Host ，这里可以进一步简化
            var host = new HostBuilder()
                .ConfigureLogging((context, factory) =>
                {
                    factory.SetMinimumLevel(LogLevel.Warning);
                    factory.AddConsole();
                })
                .ConfigureServices(service =>
                {
                    service.Configure<KafkaMessageBusOptions>(o =>
                    {
                      
                    });
                    service.AddSingleton<IMessageBus>( p =>
                    {
                        var options = new KafkaMessageBusOptions
                        {
                            ClientMode = ClientMode.Both,
                            BootStrapServers = "127.0.0.1:9092",
                            GroupId = "FTestGroup",
                            Topic = "FTestTopic",
                            AutoCommitIntervalMS = 5000,
                            AutoOffSetReset = "earliest"
                        };
                        return new KafkaMessageBus(options, p.GetRequiredService<ILoggerFactory>());
                    });

                    service.AddHostedService<MessageBusConsumeService>();
                    service.AddHostedService<MessageBusProduerService>();
                });

            host.RunConsoleAsync().Wait();
        }

        private static void TaskScheduler_UnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
        {
            try
            {
                Console.WriteLine(e.Exception.ToString());
            }
            catch
            {
            }
        }
    }
}