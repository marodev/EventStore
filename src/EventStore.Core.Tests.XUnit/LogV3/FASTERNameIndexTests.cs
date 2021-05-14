using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.LogV3;
using EventStore.Core.LogV3.FASTER;
using Xunit;

namespace EventStore.Core.Tests.XUnit.LogV3 {
	public class FASTERNameIndexTests : IDisposable {
		readonly string _outputDir = $"testoutput/{nameof(FASTERNameIndexTests)}";
		FASTERNameIndex _sut;

		public FASTERNameIndexTests() {
			TryDeleteDirectory();
			_sut = GenSut();
		}

		void TryDeleteDirectory() {
			try {
				Directory.Delete(_outputDir, recursive: true);
			} catch { }
		}

		FASTERNameIndex GenSut() => new(
			"StreamnameIndex",
			_outputDir,
			LogV3SystemStreams.FirstRealStream,
			LogV3SystemStreams.StreamInterval,
			5, 5, TimeSpan.FromSeconds(5));

		public void Dispose() {
			_sut?.Dispose();
			TryDeleteDirectory();
		}

		[Fact]
		public void sanity_check() {
			Assert.False(_sut.GetOrAddId("streamA", out var streamNumber, out var newNumber, out var newName));
			Assert.Equal("streamA", newName);
			Assert.Equal(LogV3SystemStreams.FirstRealStream, streamNumber);
			Assert.Equal(streamNumber, newNumber);
		}

		[Fact]
		public async Task can_checkpoint_log() {
			Assert.False(_sut.GetOrAddId("streamA", out _, out _, out _));
			Assert.False(_sut.GetOrAddId("streamB", out var streamNumber, out _, out _));
			await Task.Yield();
			await _sut.CheckpointLogAsync();

			_sut.Dispose();
			_sut = GenSut();
			Assert.True(_sut.GetOrAddId("streamB", out var streamNumberAfterRecovery, out _, out _));
			Assert.Equal(streamNumber, streamNumberAfterRecovery);
			Assert.False(_sut.GetOrAddId("streamC", out var streamNumberB, out _, out _));
			Assert.Equal(streamNumber + 2, streamNumberB);
		}

		[Fact]
		public async Task can_truncate_then_catchup() {
			// make sure we dont get a problem if we do something like recovery twice.
			// i.e. do one recovery where we delete the last entry, then replace it with another
			// (same number, different name), but neither the deletion or the recreation are persisted
			// now when we restart it will have diverged.

			// put a stream into the index and persist it
			_sut.GetOrAddId("streamA", out var streamAId, out _, out _);
			await _sut.CheckpointLogAsync();

			// call init with an empty source, removing streamA.
			_sut.Init(new MockNameIndexSource(new()));

			// simulate restart
			_sut.Dispose();
			_sut = GenSut();

			// when adding new stream
			_sut.GetOrAddId("streamB", out var streamBId, out _, out _);

			// expect that it has the same id as A, since that was truncated.
			Assert.Equal(streamAId, streamBId);
		}

		static readonly int _offset = (int)(LogV3SystemStreams.FirstRealStream / LogV3SystemStreams.StreamInterval);
		static readonly IEnumerable<(long StreamId, string StreamName)> _streamsSource =
			Enumerable
				.Range(_offset, int.MaxValue - _offset)
				.Select(x => (StreamId: (long)x * 2, StreamName: $"stream{x * 2}"));

		void PopulateSut(int numStreams) {
			_streamsSource.Take(numStreams).ToList().ForEach(tuple => {
				_sut.GetOrAddId(tuple.StreamName, out var streamId, out var _, out var _);
				Assert.Equal(tuple.StreamId, streamId);
			});
		}

		void DeleteStreams(int numTotalStreams, int numToDelete) {
			var streamsStream = GenerateStreamsStream(numTotalStreams - numToDelete);
			var source = new MockNameIndexSource(streamsStream.ToDictionary(x => x.StreamId, x => x.StreamName));
			_sut.Init(source);
		}

		static IList<(long StreamId, string StreamName)> GenerateStreamsStream(int numStreams) {
			return _streamsSource.Take(numStreams).ToList();
		}

		void TestInit(int numInStreamNameIndex, int numInStandardIndex) {
			// given
			PopulateSut(numInStreamNameIndex);
			var streamsStream = GenerateStreamsStream(numInStandardIndex);

			// when
			var source = new MockNameIndexSource(streamsStream.ToDictionary(x => x.StreamId, x => x.StreamName));
			_sut.Init(source);

			// then
			// now we have caught up we should be able to check that both indexes are equal
			// check that all of the streams stream is in the stream name index
			var i = 0;
			for (; i < streamsStream.Count; i++) {
				var (streamId, streamName) = streamsStream[i];
				Assert.True(_sut.GetOrAddId(streamName, out var outStreamId, out var _, out var _));
				Assert.Equal(streamId, outStreamId);
			}

			// check that the streamnameindex doesn't contain anything extra.
			foreach (var (name, value) in _sut.Scan()) {
				Assert.True(source.TryGetName(value, out var outName));
				Assert.Equal(name, outName);
			}

			// and the next created stream has the right number
			Assert.False(_sut.GetOrAddId($"{Guid.NewGuid()}", out var newStreamId, out var _, out var _));
			Assert.Equal(streamsStream.Last().StreamId + 2, newStreamId);
		}

		[Fact]
		public void on_init_can_catchup() {
			TestInit(
				numInStreamNameIndex: 3,
				numInStandardIndex: 5);
		}

		[Fact]
		public void on_init_can_catchup_from_0() {
			TestInit(
				numInStreamNameIndex: 0,
				numInStandardIndex: 5000);
		}

		[Fact]
		public void on_init_can_truncate() {
			TestInit(
				numInStreamNameIndex: 5000,
				numInStandardIndex: 3000);
		}

		[Fact]
		void can_use_read_cache_for_getoradd() {
			var numStreams = 100_000;
			PopulateSut(numStreams);

			void GetOrAddId() {
				Assert.True(_sut.GetOrAddId("stream2000", out var streamId, out _, out _));
				Assert.Equal(2000, streamId);
			}

			GetOrAddId();

			var sw = new Stopwatch();
			sw.Start();
			for (int i = 0; i < 100_000; i++)
				GetOrAddId();
			Assert.True(sw.ElapsedMilliseconds < 1000);
		}

		[Fact]
		void can_use_read_cache_for_lookup() {
			var numStreams = 100_000;
			PopulateSut(numStreams);

			Assert.Equal(2000, _sut.LookupValue("stream2000"));

			var sw = new Stopwatch();
			sw.Start();
			for (int i = 0; i < 100_000; i++)
				Assert.Equal(2000, _sut.LookupValue("stream2000"));
			Assert.True(sw.ElapsedMilliseconds < 1000);
		}

		[Fact]
		public void can_scan() {
			var numStreams = 5000;
			PopulateSut(numStreams);

			var scanned = _sut.Scan().ToList();

			Assert.Equal(numStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0; i < numStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].Value);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Name);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact]
		public void can_scan_backwards() {
			var numStreams = 5000;
			PopulateSut(numStreams);

			var scanned = _sut.ScanBackwards().ToList();
			scanned.Reverse();

			Assert.Equal(numStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0; i < numStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].Value);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Name);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact]
		public void can_scan_empty_range() {
			var numStreams = 10000;
			PopulateSut(numStreams);

			var scanned = _sut.Scan(0, 0).ToList();
			Assert.Empty(scanned);
		}

		[Fact]
		public void can_scan_forwards_skipping_truncated() {
			var numStreams = 10000;
			var deletedStreams = 500;
			var remainingStreams = numStreams - deletedStreams;
			PopulateSut(numStreams);
			DeleteStreams(numStreams, deletedStreams);

			var scanned = _sut.Scan().ToList();

			Assert.Equal(remainingStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0; i < remainingStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].Value);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Name);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact]
		public void can_scan_backwards_skipping_truncated() {
			var numStreams = 10000;
			var deletedStreams = 500;
			var remainingStreams = numStreams - deletedStreams;
			PopulateSut(numStreams);
			DeleteStreams(numStreams, deletedStreams);

			var scanned = _sut.ScanBackwards().ToList();
			scanned.Reverse();

			Assert.Equal(remainingStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0; i < remainingStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].Value);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Name);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact(Skip = "slow")]
		//[Fact]
		public void can_have_multiple_readers() {
			var numStreams = 100_000;
			PopulateSut(numStreams);

			var mres = new ManualResetEventSlim();
			void RunThread() {
				try {
					var expectedStreamId = LogV3SystemStreams.FirstRealStream;
					for (int i = 0; i < numStreams; i++) {
						var streamId = _sut.LookupValue($"stream{expectedStreamId}");
						if (streamId != expectedStreamId)
							mres.Set();
						expectedStreamId += LogV3SystemStreams.StreamInterval;
					}
				}
				catch {
					mres.Set();
				}
			}

			var t1 = new Thread(RunThread);
			var t2 = new Thread(RunThread);

			t1.Start();
			t2.Start();

			t1.Join();
			t2.Join();

			Assert.False(mres.IsSet);
		}
	}
}
