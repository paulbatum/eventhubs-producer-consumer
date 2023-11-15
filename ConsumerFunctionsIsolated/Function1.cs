using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace ConsumerFunctionsIsolated
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;
        private static TimeSpan _processingDelay = TimeSpan.FromMilliseconds(int.Parse(Environment.GetEnvironmentVariable("ProcessingDelayMilliseconds") ?? "0"));

        private static long _totalEventsProcessed = 0;
        private static long _totalBytesProcessed = 0;
        private static long _lastBytesProcessed = 0;
        private static long _batchCount = 0;

        private static Stopwatch _stopwatch = new Stopwatch();
        private static ConcurrentQueue<int> _batchSizes = new ConcurrentQueue<int>();

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        public async Task Run([EventHubTrigger("%HubName%", Connection = "EventHubConnectionString")] EventData[] events)
        {
            var localByteCount = 0;

            if (_processingDelay > TimeSpan.Zero)
            {
                await Task.Delay(_processingDelay);
            }

            foreach (EventData @event in events)
            {
                localByteCount += @event.Body.Length;
            }

            _batchSizes.Enqueue(events.Length);
            Interlocked.Increment(ref _batchCount);
            Interlocked.Add(ref _totalBytesProcessed, localByteCount);
            Interlocked.Add(ref _totalEventsProcessed, events.Length);

            if (_batchCount % 10 == 0)
            {
                _logger.LogInformation(GetStats());
            }


        }

        private string GetStats()
        {
            var newBytes = _totalBytesProcessed - _lastBytesProcessed;
            var durationSeconds = _stopwatch.Elapsed.TotalSeconds;
            var averageSpeedMegabit = newBytes * 8 / (1024.0 * 1024.0) / durationSeconds;

            List<int> values = new List<int>();
            while (_batchSizes.TryDequeue(out int batchSize))
            {
                values.Add(batchSize);
            }

            if (values.Count == 0)
            {
                return "No batches processed.";
            }

            _lastBytesProcessed = _totalBytesProcessed;
            _stopwatch.Restart();

            return $"Total events: {_totalEventsProcessed}, Total bytes: {_totalBytesProcessed}, Total megabytes processed: {Math.Round(_totalBytesProcessed / (1024.0 * 1024.0), 4)}, Min Batch: {values.Min()}, Max Batch: {values.Max()}, Bandwidth: {Math.Round(averageSpeedMegabit, 4)}Mbps.";
        }
    }
}
