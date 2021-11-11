using Azure.Messaging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using EventHubSender.App.Models;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Sender.App
{
    class Program
    {
        // connection string to the Event Hubs namespace
        private const string connectionString = "";

        // name of the event hub
        private const string eventHubName = "";

        // number of events to be sent to the event hub
        private const int numOfEvents = 1;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.
        static EventHubProducerClient producerClient;

        static async Task Main()
        {
            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= numOfEvents; i++)
            {

                var data = new CustomEventData() { id = Guid.NewGuid().ToString(), value = Guid.NewGuid().ToString() };
                //var options = new JsonSerializerOptions { WriteIndented = true };
                //string jsonString = JsonSerializer.Serialize(data, options);

                CloudEvent cloudEvent = new CloudEvent("Sender.App", "Custom.Event.Created", data)
                {
                    Id = DateTime.Now.ToString(),
                    Subject = "custom event created",
                    DataSchema = "#"
                };

                var options = new JsonSerializerOptions { WriteIndented = true };
                var jsonString = JsonSerializer.Serialize(cloudEvent, options);
                Console.WriteLine(jsonString);

                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(jsonString))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {jsonString} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine("A batch of events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }
    }
}
