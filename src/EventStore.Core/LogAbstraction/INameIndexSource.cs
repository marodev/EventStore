namespace EventStore.Core.LogAbstraction {
	public interface INameIndexSource<TKey> {
		bool TryGetName(TKey key, out string name);

		/// returns false if there is no max (i.e. the source is empty)
		bool TryGetMaxKey(out TKey max);
	}
}
