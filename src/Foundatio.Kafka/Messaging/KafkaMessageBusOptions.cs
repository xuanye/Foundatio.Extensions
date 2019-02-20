using System;
using System.Collections.Generic;
using System.Text;

namespace Foundatio.Messaging
{
    public class KafkaMessageBusOptions : SharedMessageBusOptions
    {
        /// <summary>
        /// bootstrap.servers
        /// </summary>
        /// <value>
        /// The boot strap servers.
        /// </value>
        public string BootStrapServers { get; set; }

        /// <summary>
        /// group.id
        /// </summary>
        /// <value>
        /// The group identifier.
        /// </value>
        public string GroupId { get; set; }

        /// <summary>
        /// { "auto.commit.interval.ms", 5000 },
        /// </summary>
        /// <value>
        /// The automatic commit interval ms.
        /// </value>
        public int AutoCommitIntervalMS { get; set; } = 5000;

        /// <summary>
        /// { "auto.offset.reset", "earliest" }
        /// </summary>
        /// <value>
        /// The automatic off set reset.
        /// </value>
        public string AutoOffSetReset { get; set; }

        public IDictionary<string, Object> Arguments { get; set; }

        public ClientMode ClientMode { get; set; } = ClientMode.Both;
    }

    [Flags]
    public enum ClientMode
    {
        Producer = 1,
        Consumer = 2,
        Both = 3
    }

    public class KafkaMessageBusOptionsBuilder : SharedMessageBusOptionsBuilder<KafkaMessageBusOptions, KafkaMessageBusOptionsBuilder>
    {
        public KafkaMessageBusOptionsBuilder BootStrapServers(string bootstrapServers)
        {
            Target.BootStrapServers = bootstrapServers ?? throw new ArgumentNullException(nameof(bootstrapServers));
            return this;
        }

        public KafkaMessageBusOptionsBuilder GroupId(string groupId)
        {
            Target.GroupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
            return this;
        }

        public KafkaMessageBusOptionsBuilder AutoCommitIntervalMS(int autoCommitIntervalMS)
        {
            Target.AutoCommitIntervalMS = autoCommitIntervalMS;
            return this;
        }

        public KafkaMessageBusOptionsBuilder ClientMode(ClientMode mode)
        {
            Target.ClientMode = Target.ClientMode | mode;
            return this;
        }

        public KafkaMessageBusOptionsBuilder AutoOffSetReset(string autoOffSetReset)
        {
            Target.AutoOffSetReset = autoOffSetReset ?? throw new ArgumentNullException(nameof(autoOffSetReset));
            return this;
        }

        public KafkaMessageBusOptionsBuilder Arguments(IDictionary<string, object> arguments)
        {
            Target.Arguments = arguments ?? throw new ArgumentNullException(nameof(arguments));
            return this;
        }
    }
}