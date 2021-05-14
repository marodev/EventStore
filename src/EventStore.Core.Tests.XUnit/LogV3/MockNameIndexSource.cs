using System.Collections.Generic;
using System.Linq;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.Tests.XUnit.LogV3 {
	class MockNameIndexSource : INameIndexSource<long> {
		private readonly Dictionary<long, string> _dict;

		public MockNameIndexSource(Dictionary<long, string> dict) {
			_dict = dict;
		}

		public bool TryGetMaxKey(out long max) {
			if (_dict.Count == 0) {
				max = 0;
				return false;
			}

			max = _dict.Keys.Max();
			return true;
		}

		public bool TryGetName(long key, out string name) {
			return _dict.TryGetValue(key, out name);
		}
	}
}
