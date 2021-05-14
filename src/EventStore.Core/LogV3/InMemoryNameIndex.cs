using System;
using System.Collections.Concurrent;
using EventStore.Common.Utils;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.LogV3 {
	// temporary implementation as stepping stone
	public class InMemoryNameIndex :
		INameIndex<long>,
		IValueLookup<long>,
		INameLookup<long> {

		long _next;
		readonly ConcurrentDictionary<string, long> _dict = new ConcurrentDictionary<string, long>();
		readonly ConcurrentDictionary<long, string> _rev = new ConcurrentDictionary<long, string>();

		public InMemoryNameIndex(long firstValue) {
			_next = firstValue;
		}

		public void Init(INameIndexSource<long> source) {
			throw new NotImplementedException();
		}

		public bool GetOrAddId(string name, out long value, out long addedValue, out string addedName) {
			Ensure.NotNullOrEmpty(name, "name");

			var oldNext = _next;
			value = _dict.GetOrAdd(name, n => {
				_next += 2;
				var v = _next;
				_dict[n] = v;
				_rev[v] = n;
				return v;
			});

			// return true if we found an existing entry. i.e. did not have to allocate from _next
			addedValue = value;
			addedName = name;
			return oldNext == _next;
		}

		public long LookupValue(string name) {
			if (!_dict.TryGetValue(name, out var value))
				return 0;

			return value;
		}

		// todo: will be able to read from the index once we have stream records
		// (even in mem), at which point we can drop implementation of INameLookup here.
		public string LookupName(long value) {
			_rev.TryGetValue(value, out var name);
			return name ?? "";
		}
	}
}
