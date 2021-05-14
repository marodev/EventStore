using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Unicode;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.DataStructures;
using EventStore.Core.LogAbstraction;
using FASTER.core;
using Serilog;
using Value = System.Int64;

namespace EventStore.Core.LogV3.FASTER {
	public class NameIndex {
		protected static readonly ILogger Log = Serilog.Log.ForContext<NameIndex>();
	}

	public class FASTERNameIndex :
		NameIndex,
		INameIndex<long>,
		IValueLookup<long> {

		private static readonly Encoding _utf8NoBom = new UTF8Encoding(false, true);
		private readonly IDevice _log;
		private readonly FasterKV<SpanByte, Value> _store;
		private readonly LogSettings _logSettings;
		readonly Debouncer _logCheckpointer;

		// used during initialisation
		private readonly ClientSession<SpanByte, Value, Value, Value, Empty, MaintenanceFunctions<Value>> _maintenanceSession;

		// used by the storage writer, single thread.
		private readonly ClientSession<SpanByte, Value, Value, Value, Context<Value>, WriterFunctions<Value>> _writerSession;
		private readonly Context<Value> _writerContext = new();

		// used by various reader threads and during intitialisation
		private readonly ObjectPool<ReaderSession<Value>> _readerSessionPool;
		private readonly string _indexName;
		private readonly Value _firstValue;
		private readonly Value _valueInterval;
		private Value _nextValue;

		public FASTERNameIndex(
			string indexName,
			string logDir,
			Value firstValue,
			Value valueInterval,
			int initialReaderCount,
			int maxReaderCount,
			TimeSpan checkpointInterval) {

			_indexName = indexName;
			_firstValue = firstValue;
			_valueInterval = valueInterval;
			_nextValue = firstValue;

			_log = Devices.CreateLogDevice(
				logPath: $"{logDir}/{_indexName}.log",
				deleteOnClose: false);

			var checkpointSettings = new CheckpointSettings {
				CheckpointDir = $"{logDir}",
				RemoveOutdated = true,
			};

			_logSettings = new LogSettings {
				LogDevice = _log,
				// todo: dynamic settings according to available memory
				MemorySizeBits = 15,
				PageSizeBits = 12,
				//PreallocateLog = true,

				ReadCacheSettings = new ReadCacheSettings {
					// todo: dynamic settings according to available memory
					//MemorySizeBits = 15,
					//PageSizeBits = 12,
					//SecondChanceFraction = 0.1,
				}
			};

			_store = new FasterKV<SpanByte, Value>(
				// todo: dynamic settings according to available memory
				// but bear in mind if we have taken an index checkpoint there is a 
				// procedure to resize it.
				size: 1L << 20,
				checkpointSettings: checkpointSettings,
				logSettings: _logSettings);

			_writerSession = _store
				.For(new WriterFunctions<Value>())
				.NewSession<WriterFunctions<Value>>();

			_maintenanceSession = _store
				.For(new MaintenanceFunctions<Value>())
				.NewSession<MaintenanceFunctions<Value>>();

			_readerSessionPool = new ObjectPool<ReaderSession<Value>>(
				objectPoolName: $"{_indexName} readers pool",
				initialCount: initialReaderCount,
				maxCount: maxReaderCount,
				factory: () => new ReaderSession<Value>(
					_store.For(new ReaderFunctions<Value>()).NewSession<ReaderFunctions<Value>>(),
					new Context<Value>()),
				dispose: session => session.Dispose());

			_logCheckpointer = new Debouncer(
				checkpointInterval,
				async token => {
					try {
						var success = await CheckpointLogAsync().ConfigureAwait(false);
						if (!success)
							throw new Exception($"not successful");

						Log.Debug("{_indexName} took checkpoint", _indexName);
					} catch (Exception ex) {
						Log.Error(ex, "{_indexName} could not take checkpoint", _indexName);
					}
				},
				CancellationToken.None);

			Log.Information("{indexName} base memory {totalMemoryMib:N0}MiB.", _indexName, CalcTotalMemoryMib());
			Recover();
			Log.Information("{indexName} total memory after recovery {totalMemoryMib:N0}MiB.", _indexName, CalcTotalMemoryMib());
		}

		public void Dispose() {
			_maintenanceSession?.Dispose();
			_writerSession?.Dispose();
			_readerSessionPool?.Dispose();
			_store?.Dispose();
			_log?.Dispose();
		}

		void Recover() {
			try {
				_store.Recover();
				var (key, value) = ScanBackwards().FirstOrDefault();
				if (key != null)
					_nextValue = value + _valueInterval;
				Log.Information(
					"{indexName} has been recovered. " +
					"Last entry was \"{key}\":{value}. nextValue is {nextValue}",
					_indexName, key, value, _nextValue);
			} catch (FasterException) {
				Log.Information($"{_indexName} is starting from scratch");
			}
		}

		// the source has been initialised. it now contains exactly the data that we want to have in the name index.
		// there are three cases
		// 1. we are exactly in sync with the source (do nothing)
		// 2. we are behind the source (could be by a lot, say we are rebuilding) -> catch up
		// 3. we are ahead of the source (should only be by a little) -> truncate
		public void Init(INameIndexSource<Value> source) {
			Log.Information("{indexName} initializing...", _indexName);
			var iter = ScanBackwards().GetEnumerator();

			if (!iter.MoveNext()) {
				Log.Information("{indexName} is empty. Catching up from beginning of source.", _indexName);
				CatchUp(source, previousValue: 0);

			} else if (source.TryGetName(iter.Current.Value, out var sourceName)) {
				if (sourceName != iter.Current.Name)
					ThrowNameMismatch(iter.Current.Value, iter.Current.Name, sourceName);

				Log.Information("{indexName} has entry {value}. Catching up from there", _indexName, iter.Current.Value);
				CatchUp(source, previousValue: iter.Current.Value);

			} else {
				// we have a most recent entry but it is not in the source.
				// scan backwards until we find something in common with the source and
				// delete everything in between.
				var keysToDelete = new List<string>();
				void PopEntry() {
					keysToDelete.Add(iter.Current.Name);
					_nextValue -= _valueInterval;
					if (_nextValue != iter.Current.Value)
						throw new Exception($"{_indexName} this should never happen {_nextValue} {iter.Current.Value}");
				}

				PopEntry();

				bool found = false;
				while (!found && iter.MoveNext()) {
					found = source.TryGetName(iter.Current.Value, out sourceName);
					if (!found) {
						PopEntry();
					}
				}

				if (found) {
					source.TryGetMaxKey(out var max);
					if (iter.Current.Value != max)
						throw new Exception($"{_indexName} this should never happen. expecting source to have values up to {iter.Current.Value} but was {max}");
					if (iter.Current.Name != sourceName)
						ThrowNameMismatch(iter.Current.Value, iter.Current.Name, sourceName);
				} else {
					if (source.TryGetMaxKey(out var max))
						throw new Exception($"{_indexName} this should never happen. expecting source to have been empty but it wasn't. has values up to {max}");
				}

				Log.Information("{indexName} is truncating {count} entries", _indexName, keysToDelete.Count);
				Truncate(keysToDelete);
			}

			CheckpointLogSynchronously();
			Log.Information("{indexName} initialized.", _indexName);
			Log.Information("{indexName} total memory after initialization {totalMemoryMib:N0}MiB.", _indexName, CalcTotalMemoryMib());
		}

		void ThrowNameMismatch(Value value, string ourName, string sourceName) {
			throw new Exception(
				$"{_indexName} this should never happen. name mismatch. " +
				$"value: {value} name: \"{ourName}\"/\"{sourceName}\"");
		}

		void ThrowValueMismatch(string name, Value ourValue, Value sourceValue) {
			throw new Exception(
				$"{_indexName} this should never happen. value mismatch. " +
				$"name: \"{name}\" value: {ourValue}/{sourceValue}");
		}

		void CatchUp(INameIndexSource<Value> source, Value previousValue) {
			if (!source.TryGetMaxKey(out var max))
				return;

			var startValue = previousValue == 0
				? _firstValue
				: previousValue + _valueInterval;

			var count = 0;
			for (var sourceValue = startValue; sourceValue <= max; sourceValue += _valueInterval) {
				count++;
				if (!source.TryGetName(sourceValue, out var name))
					throw new Exception($"{_indexName} this should never happen. could not find {sourceValue} in source");

				var preExisting = GetOrAddId(name, out var value, out var _, out var _);

				if (preExisting)
					throw new Exception(
						$"{_indexName} this should never happen. found preexisting name when catching up. " +
						$"name: \"{name}\" value: {sourceValue}/{value}");
				if (value % _valueInterval != 0)
					throw new Exception(
						$"{_indexName} this should never happen. found value inbetween interval. " +
						$"name: \"{name}\" value: {sourceValue}/{value}");
				if (value != sourceValue)
					ThrowValueMismatch(name, value, sourceValue);
			}

			Log.Information("{indexName} caught up {count} entries", _indexName, count);
		}

		void Truncate(IList<string> names) {
			for (int i = 0; i < names.Count; i++) {
				Truncate(names[i]);
			}
		}

		// must be truncating from the tail
		void Truncate(string name) {
			if (string.IsNullOrEmpty(name))
				throw new ArgumentNullException(nameof(name));

			Log.Information("{indexName} is deleting key \"{key}\"", _indexName, name);

			// convert the name into a UTF8 span which we can use as a key.
			Span<byte> keySpan = stackalloc byte[Measure(name)];
			PopulateKey(name, keySpan);

			var status = _maintenanceSession.Delete(key: SpanByte.FromFixedSpan(keySpan));

			switch (status) {
				case Status.OK:
				case Status.NOTFOUND:
					break;

				case Status.PENDING:
					_maintenanceSession.CompletePending(wait: true);
					break;

				default:
					throw new Exception($"{_indexName} Unexpected status {status} deleting {name}");
			}
		}

		public IEnumerable<(string Name, Value Value)> Scan() => Scan(0, _store.Log.TailAddress);

		public IEnumerable<(string Name, Value Value)> Scan(long beginAddress, long endAddress) {
			using var iter = _store.Log.Scan(beginAddress, endAddress);
			while (iter.GetNext(out var recordInfo)) {
				if (recordInfo.Invalid || recordInfo.Tombstone)
					continue;

				var key = iter.GetKey();
				var value = iter.GetValue();
				var keyString = _utf8NoBom.GetString(key.AsReadOnlySpan());
				var currentValue = LookupValue(keyString);

				if (currentValue == 0) {
					// deleted (from a previous truncation)
					continue;
				}
				else if (currentValue != value) {
					throw new Exception($"{_indexName} this should never happen. value was updated from {value} to {currentValue}");
				}

				yield return (keyString, value);
			}
		}

		public IEnumerable<(string Name, Value Value)> ScanBackwards() {
			// in faster you can begin log scans from records address, but also from
			// page boundaries. use this to iterate backwards by jumping back
			// one page at a time.
			var pageSize = 1 << _logSettings.PageSizeBits;
			var endAddress = _store.Log.TailAddress;
			var beginAddress = endAddress / pageSize * pageSize;

			if (beginAddress < 0)
				beginAddress = 0;

			var detailCount = 0;
			while (endAddress > 0) {
				var entries = Scan(beginAddress, endAddress).ToList();
				for (int i = entries.Count - 1; i >= 0; i--) {
					yield return entries[i];
					detailCount++;
				}
				endAddress = beginAddress;
				beginAddress -= pageSize;
			}
		}

		public Value LookupValue(string name) {
			using var lease = _readerSessionPool.Rent();
			var session = lease.Reader.ClientSession;
			var context = lease.Reader.Context;

			// convert the name into a UTF8 span which we can use as a key.
			Span<byte> key = stackalloc byte[Measure(name)];
			PopulateKey(name, key);

			var status = session.Read(
				key: SpanByte.FromFixedSpan(key),
				output: out var value,
				userContext: context);

			switch (status) {
				case Status.OK: return value;
				case Status.NOTFOUND: return 0;
				case Status.PENDING:
					session.CompletePending(wait: true);
					switch (context.Status) {
						case Status.OK: return context.Value;
						case Status.NOTFOUND: return 0;
						default: throw new Exception($"{_indexName} Unexpected status {context.Status} completing read for \"{name}\"");
					}
				default: throw new Exception($"{_indexName} Unexpected status {status} reading \"{name}\"");
			}
		}

		public bool GetOrAddId(string name, out Value value, out Value addedValue, out string addedName) {
			if (string.IsNullOrEmpty(name))
				throw new ArgumentNullException(nameof(name));

			// convert the name into a UTF8 span which we can use as a key in FASTER.
			Span<byte> key = stackalloc byte[Measure(name)];
			PopulateKey(name, key);

			// read to see if we already have this name
			bool preExisting;
			var status = _writerSession.Read(
				key: SpanByte.FromFixedSpan(key),
				output: out value,
				userContext: _writerContext);

			switch (status) {
				case Status.OK:
					preExisting = true;
					break;

				case Status.NOTFOUND:
					preExisting = false;
					AddNew(key, out value);
					break;

				case Status.PENDING:
					_writerSession.CompletePending(wait: true);
					switch (_writerContext.Status) {
						case Status.OK:
							preExisting = true;
							value = _writerContext.Value;
							break;

						case Status.NOTFOUND:
							preExisting = false;
							AddNew(key, out value);
							break;

						default:
							throw new Exception($"{_indexName} Unexpected status {_writerContext.Status} completing get/add for \"{name}\"");
					}
					break;

				default:
					throw new Exception($"{_indexName} Unexpected status {status} get/adding \"{name}\"");
			}

			addedValue = value;
			addedName = name;

			if (!preExisting) {
				Log.Debug("{indexName} added new entry: \"{key}\":{value}", _indexName, name, value);
				_logCheckpointer.Trigger();
			}

			return preExisting;
		}

		void AddNew(Span<byte> key, out Value value) {
			value = _nextValue;
			_writerSession.Upsert(
				key: SpanByte.FromFixedSpan(key),
				desiredValue: value);
			_nextValue += _valueInterval;
		}

		void CheckpointLogSynchronously() {
			// unideal
			CheckpointLogAsync().GetAwaiter().GetResult();
		}

		public async Task<bool> CheckpointLogAsync() {
			LogStats();
			Log.Debug("{indexName} is checkpointing", _indexName);
			var (success, _) = await _store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
			return success;
		}

		void LogStats() {
			Log.Debug("{indexName} total memory {totalMemoryMib:N0}MiB.", _indexName, CalcTotalMemoryMib());
		}

		long CalcTotalMemoryMib() {
			var indexSizeBytes = _store.IndexSize * 64;
			var indexOverflowBytes = _store.OverflowBucketCount * 64;
			var logMemorySizeBytes = _store.Log.MemorySizeBytes;
			var readCacheMemorySizeBytes = _store.ReadCache?.MemorySizeBytes ?? 0;
			var totalMemoryBytes = indexSizeBytes + indexOverflowBytes + logMemorySizeBytes + readCacheMemorySizeBytes;
			var totalMemoryMib = totalMemoryBytes / 1024 / 1024;
			return totalMemoryMib;
		}

		static int Measure(string source) {
			var length = _utf8NoBom.GetByteCount(source);
			return length;
		}

		static void PopulateKey(string source, Span<byte> destination) {
			Utf8.FromUtf16(source, destination, out _, out _, true, true);
		}
	}
}
