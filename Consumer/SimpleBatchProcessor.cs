using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;

namespace Consumer
{
    public class SimpleBatchProcessor : PluggableCheckpointStoreEventProcessor<EventProcessorPartition>
    {
        // This example uses a connection string, so only the single constructor
        // was implemented; applications will need to shadow each constructor of
        // the EventProcessorClient that they are using.

        private long _totalEventsProcessed = 0;
        private long _totalBytesProcessed = 0;
        private long _lastBytesProcessed = 0;

        private Stopwatch _stopwatch = new Stopwatch();
        private ConcurrentQueue<int> _batchSizes = new ConcurrentQueue<int>();

        public TimeSpan ProcessingDelay { get; set; } = TimeSpan.Zero;

        public SimpleBatchProcessor(CheckpointStore checkpointStore,
                                    int eventBatchMaximumCount,
                                    string consumerGroup,
                                    string connectionString,
                                    string eventHubName,
                                    EventProcessorOptions clientOptions = default)
            : base(
                checkpointStore,
                eventBatchMaximumCount,
                consumerGroup,
                connectionString,
                eventHubName,
                clientOptions)
        {
        }

        public override Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            _stopwatch.Start();
            return base.StartProcessingAsync(cancellationToken);
        }

        protected override async Task OnProcessingEventBatchAsync(IEnumerable<EventData> events,
                                                                  EventProcessorPartition partition,
                                                                  CancellationToken cancellationToken)
        {
            // Like the event handler, it is very important that you guard
            // against exceptions in this override; the processor does not
            // have enough understanding of your code to determine the correct
            // action to take.  Any exceptions from this method go uncaught by
            // the processor and will NOT be handled.  The partition processing
            // task will fault and be restarted from the last recorded checkpoint.

            try
            {
                if (ProcessingDelay > TimeSpan.Zero)
                {
                    await Task.Delay(ProcessingDelay);
                }

                var localEventCount = 0;
                var localByteCount = 0;
                foreach (EventData eventData in events)
                {
                    localEventCount++;
                    localByteCount += eventData.Body.Length;
                }

                _batchSizes.Enqueue(localEventCount);
                Interlocked.Add(ref _totalBytesProcessed, localByteCount);
                Interlocked.Add(ref _totalEventsProcessed, localEventCount);

                // Create a checkpoint based on the last event in the batch.
                var lastEvent = events.Last();

                await UpdateCheckpointAsync(
                    partition.PartitionId,
                    lastEvent.Offset,
                    lastEvent.SequenceNumber,
                    cancellationToken);
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Exception in {nameof(OnProcessingEventBatchAsync)}: {ex}");
            }

            // Calling the base would only invoke the process event handler and provide no
            // value; we will not call it here.
        }

        protected override Task OnProcessingErrorAsync(Exception exception,
                                                             EventProcessorPartition partition,
                                                             string operationDescription,
                                                             CancellationToken cancellationToken)
        {
            // Like the event handler, it is very important that you guard
            // against exceptions in this override; the processor does not
            // have enough understanding of your code to determine the correct
            // action to take.  Any exceptions from this method go uncaught by
            // the processor and will NOT be handled.  Unhandled exceptions will
            // not impact the processor operation but will go unobserved, hiding
            // potential application problems.

            Console.Error.WriteLine($"Processing error: {exception}");
            return Task.CompletedTask;
        }

        public string GetStats()
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
