using Confluent.Kafka;

namespace SimpleApiDotnet.Services;

public class KafkaProducerService
{
    public readonly ILogger<KafkaProducerService> _logger;
    public readonly IProducer<Null, string> _producer;
    public readonly string _topicName;

    public KafkaProducerService(IConfiguration configuration, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _topicName = configuration["Kafka:TopicName"] ?? "product-events";

        var config = new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"]
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
        _logger.LogInformation($"Producer started for topic {_topicName} and server {config.BootstrapServers}");
    }

    public async Task ProduceAsync(string message)
    {
        try
        {
            var deliveryResult =
                await _producer.ProduceAsync(_topicName, new Message<Null, string> { Value = message });
            _logger.LogInformation($"Message {message} written to topic {_topicName}");
        }
        catch (ProduceException<Null, string> e)
        {
            _logger.LogError(e.Error.Reason);
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}