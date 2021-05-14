namespace EventStore.Core.LogAbstraction {
	/// Looks up a name given a value
	public interface INameLookup<TValue> {
		string LookupName(TValue value);
	}
}
