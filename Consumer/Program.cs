using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Storage.Blobs;

namespace Consumer
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var storageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
            var eventHubConnectionString = Environment.GetEnvironmentVariable("EventHubConnectionString");
            var eventHubName = Environment.GetEnvironmentVariable("HubName");

            if (string.IsNullOrEmpty(storageConnectionString) || string.IsNullOrEmpty(eventHubConnectionString) || string.IsNullOrEmpty(eventHubName))
            {
                throw new Exception("Please set StorageConnectionString, EventHubConnectionString and HubName environment variables");
            }

            var blobContainerName = Environment.GetEnvironmentVariable("BlobContainerName") ?? "checkpoints";
            string consumerGroup = Environment.GetEnvironmentVariable("ConsumerGroup") ?? EventHubConsumerClient.DefaultConsumerGroupName;

            int maximumBatchSize = int.Parse(Environment.GetEnvironmentVariable("MaximumBatchSize") ?? "5000");
            int prefetchCount = int.Parse(Environment.GetEnvironmentVariable("PrefetchCount") ?? "20000");
            var processingDelay = TimeSpan.FromMilliseconds(int.Parse(Environment.GetEnvironmentVariable("ProcessingDelayMilliseconds") ?? "0"));

            var storageClient = new BlobContainerClient(storageConnectionString, blobContainerName);
            var checkpointStore = new BlobCheckpointStore(storageClient);
            var options = new EventProcessorOptions { PrefetchCount = prefetchCount };

            var processor = new SimpleBatchProcessor(
                checkpointStore,
                maximumBatchSize,
                consumerGroup,
                eventHubConnectionString,
                eventHubName,
                options);

            processor.ProcessingDelay = processingDelay;

            using var cancellationSource = new CancellationTokenSource();

            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true;
                cancellationSource.Cancel();
            };

            async Task PrintStatsAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    Console.WriteLine(processor.GetStats());
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
            }

            try
            {
                var tasks = new List<Task>
                {
                    PrintStatsAsync(cancellationSource.Token),
                    processor.StartProcessingAsync(cancellationSource.Token)
                };

                await Task.WhenAll(tasks);
                await Task.Delay(Timeout.Infinite, cancellationSource.Token);
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                // Stopping may take up to the length of time defined
                // as the TryTimeout configured for the processor;
                // By default, this is 60 seconds.

                await processor.StopProcessingAsync();
            }
        }
    }
}