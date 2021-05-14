namespace EventStore.Core.LogAbstraction {
	/// Maps names (strings) to TValues
	public interface INameIndex<TValue> {
		void Init(INameIndexSource<TValue> source);
		// return true => stream already existed.
		// return false => stream was created. createdId and createdName are the details of the created stream.
		// these can be different to streamName/streamId e.g. if streamName is a metastream.
		bool GetOrAddId(string name, out TValue value, out TValue addedValue, out string addedName);
	}
}
