
using System.Diagnostics;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace Producer
{
    internal class Program
    {
        async static Task Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("EventHubConnectionString");
            var hubName = Environment.GetEnvironmentVariable("HubName");

            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(hubName))
            {
                throw new Exception("Please set EventHubConnectionString and HubName environment variables");
            }

            var messagesPerBatch = int.Parse(Environment.GetEnvironmentVariable("MessagesPerBatch") ?? "100");
            var messageBodySizeBytes = int.Parse(Environment.GetEnvironmentVariable("MessageBodySizeBytes") ?? "1024");
            var sendBatchDelay = TimeSpan.FromMilliseconds(int.Parse(Environment.GetEnvironmentVariable("SendBatchDelayMilliseconds") ?? "1000"));
            var parallelSendsPerPartition = int.Parse(Environment.GetEnvironmentVariable("ParallelSendsPerPartition") ?? "1");

            var buffers = new List<byte[]>();
            for (int i = 0; i < messagesPerBatch; i++)
            {
                var buff = new byte[messageBodySizeBytes];
                Random.Shared.NextBytes(buff);
                buffers.Add(buff);
            }

            var data = buffers
                .Select(x => new ReadOnlyMemory<byte>(x))
                .Select(x => new EventData(x)).ToList();

            var clients = new List<EventHubProducerClient>();

            for (int i = 0; i < parallelSendsPerPartition; i++)
            {
                clients.Add(new EventHubProducerClient(connectionString, hubName));
            }

            var properties = await clients[0].GetEventHubPropertiesAsync();

            ulong sentBytes = 0;
            int batchesSent = 0;
            double averageSpeedMegabit = 0;
            var watch = Stopwatch.StartNew();
            Console.WriteLine($"Sending data to {properties.Name} on partitions: {string.Join(',', properties.PartitionIds)} with {parallelSendsPerPartition} threads per partition");

            async Task SendLoopAsync(EventHubProducerClient producerClient, string partitionId)
            {
                while (true)
                {
                    using var batch = await producerClient.CreateBatchAsync(new CreateBatchOptions { PartitionId = partitionId });

                    foreach (var d in data)
                    {
                        if (batch.TryAdd(d) == false)
                            throw new Exception($"Problem with batchsize, maxSize: {batch.MaximumSizeInBytes}, batchSize: {batch.SizeInBytes} ");
                    }

                    await producerClient.SendAsync(batch);

                    Interlocked.Add(ref sentBytes, Convert.ToUInt64(batch.SizeInBytes));

                    var batchCount = Interlocked.Increment(ref batchesSent);

                    if (batchCount % 10 == 0)
                    {
                        Console.WriteLine($"Sent {batchesSent} batches. Total of {sentBytes} bytes, {Math.Round(sentBytes / (1024.0 * 1024.0), 4)} megabytes. Average speed: {Math.Round(averageSpeedMegabit, 4)}Mbps");
                    }

                    if (sendBatchDelay > TimeSpan.Zero)
                    {
                        await Task.Delay(sendBatchDelay);
                    }
                }
            }

            var tasks = new List<Task>();

            foreach (var c in clients)
            {
                foreach (var p in properties.PartitionIds)
                {
                    tasks.Add(SendLoopAsync(c, p));
                }
            }

            async Task ResetStatsLoopAsync()
            {
                ulong lastSentBytes = 0;

                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    var newBytes = sentBytes - lastSentBytes;
                    var durationSeconds = watch.Elapsed.TotalSeconds;

                    averageSpeedMegabit = newBytes * 8 / (1024.0 * 1024.0) / durationSeconds;
                    lastSentBytes = sentBytes;
                    watch.Restart();
                }
            }

            Console.WriteLine($"Total parallel sends: {tasks.Count}");
            tasks.Add(ResetStatsLoopAsync());

            await Task.WhenAll(tasks);

        }
    }
}