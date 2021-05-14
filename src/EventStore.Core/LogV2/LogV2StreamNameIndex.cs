using EventStore.Common.Utils;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.LogV2 {
	public class LogV2StreamNameIndex :
		INameIndex<string>,
		IValueLookup<string>,
		INameLookup<string> {

		public LogV2StreamNameIndex() {
		}

		public void Init(INameIndexSource<string> source) {
		}

		public bool GetOrAddId(string streamName, out string streamId, out string createdId, out string createdName) {
			Ensure.NotNullOrEmpty(streamName, "streamName");
			streamId = streamName;
			createdId = default;
			createdName = default;
			return true;
		}

		public string LookupValue(string streamName) => streamName;
		public string LookupName(string streamName) => streamName;
	}
}
