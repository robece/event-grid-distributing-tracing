using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Consumer.Function
{
    public class LastConsumer
    {
        [FunctionName("LastConsumer")]
        public static async Task Run([EventHubTrigger("", Connection = "EventHubConnectionString", ConsumerGroup = "")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.EventBody);
                    string properties = string.Join(";", eventData.Properties.Select(x => x.Key + "=" + x.Value).ToArray());
                    string systemProperties = string.Join(";", eventData.SystemProperties.Select(x => x.Key + "=" + x.Value).ToArray());

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"Message Body: {messageBody}");
                    log.LogInformation($"Properties: {properties}");
                    log.LogInformation($"System Properties: {systemProperties}");

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
