package net.uresk.samples.elastic.aliasing;

public interface IndexRebuildJob<T>
{

	public T rebuildIndex(String indexName);
	
}
