using Azure;
using Azure.Core.Serialization;
using Azure.Messaging;
using Azure.Messaging.EventGrid;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Consumer.Function
{
    public static class Consumer
    {
        [FunctionName("Consumer")]
        public static async Task Run([EventHubTrigger("topic01hub", Connection = "EventHubConnectionString")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            var myCustomDataSerializer = new JsonObjectSerializer(
            new JsonSerializerOptions()
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.EventBody);

                    string topicEndpoint = "";
                    string topicAccessKey = "";

                    var builder = new EventGridSasBuilder(new Uri(topicEndpoint), DateTimeOffset.Now.AddHours(1));
                    var keyCredential = new AzureKeyCredential(topicAccessKey);
                    string sasToken = builder.GenerateSas(keyCredential);

                    var sasCredential = new AzureSasCredential(sasToken);
                    EventGridPublisherClient client = new EventGridPublisherClient(new Uri(topicEndpoint), sasCredential);

                    // Add CloudEvents to a list to publish to the topic
                    List<CloudEvent> eventsList = new List<CloudEvent>
                    {
                        // CloudEvent with custom model serialized to JSON using a custom serializer
                        new CloudEvent(
                            "/cloudevents/example/source",
                            "Example.EventType",
                            myCustomDataSerializer.Serialize(messageBody),
                            "application/json"),
                    };

                    // Send the events
                    await client.SendEventsAsync(eventsList);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }

                if (exceptions.Count > 1)
                    throw new AggregateException(exceptions);

                if (exceptions.Count == 1)
                    throw exceptions.Single();
            }
        }
    }
}