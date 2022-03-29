using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Blobs.Models;
using System.Threading;

namespace AzureEventHubListener
{
    class MyDataCollector
    {
        public int start_epoch { get; set; }
        public int end_epoch { get; set; }
        public List<string> data;

        public MyDataCollector()
        {
            start_epoch = (int)((DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds);
            end_epoch = start_epoch + 60; // 1 minute
            data = new List<string>();
        }

        public int fetch_collector_end_time()
        {
            return end_epoch;
        }

        public bool shouldStore(int threashold)
        {
            if (data.Count >= threashold)
                return true;
            else
                return false;
        }

        public List<String> getFullData()
        {
            return data;
        }

        public void reset_with_epoch(int new_epoch)
        {
            start_epoch = new_epoch;
            end_epoch = start_epoch + 60;
            data.Clear();
        }

        public void store(String line)
        {
            data.Add(line);
        }
    }

    class Program
    {

        private const string ehubNamespaceConnectionString = "<Event Hub connection string>";
        private const string eventHubName = "<Event Hub Identity Name>";
        private const string blobStorageConnectionString = "<Blob Storage Connection string>";
        private const string blobContainerName = "<Blob Container Name>";
        static BlobContainerClient storageClient;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.        
        static EventProcessorClient processor;
        static MyDataCollector custom_collector;


        static async Task Main()
        {
            // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            //To collect data and evaluate it.
            custom_collector = new MyDataCollector();

            // Create an event processor client to process events in the event hub
            processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 30 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(300));

            // Stop the processing
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            String incoming_data = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
            Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
            int processing_epoch = (int)((DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds);
            Console.WriteLine("Processing Time:" + processing_epoch.ToString() + " Bucket End Epoch:" + custom_collector.fetch_collector_end_time().ToString());
            if(processing_epoch > custom_collector.fetch_collector_end_time())
            {
                if (custom_collector.shouldStore(5))//here 10 is threashold
                {
                    List<String> lines_to_write = custom_collector.getFullData();
                    String path = "C:\\Users\\sharmavijay\\SampleFiles";
                    String file_name = new string(processing_epoch.ToString() + ".txt");
                    File.WriteAllLines(path + file_name, lines_to_write);
                    BlobClient blb = storageClient.GetBlobClient("Failed_data_upload" + processing_epoch.ToString()+".txt");
                    await blb.UploadAsync(path + file_name);
                    BlobProperties prop = await blb.GetPropertiesAsync();
                    Console.WriteLine("Uploaded file " + "Failed_data_upload"+processing_epoch.ToString()+".txt" + " size:" + prop.ContentLength);
                    
                }
                Console.WriteLine("Reset collector.");
                custom_collector.reset_with_epoch(processing_epoch);
            }
            else
            {
                custom_collector.store(incoming_data);
                Console.WriteLine("Added_to_Store");
            }
            Thread.Sleep(3000);
            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
