package net.uresk.samples.elastic.aliasing;

import java.util.HashMap;
import java.util.Map;

public class AliasedIndexConfiguration
{

	private String aliasName;
	
	private int shardCount = 1;
	
	private int replicaCount = 0;
	
	private int indexCount = 2;
	
	public AliasedIndexConfiguration(String aliasName)
	{
		this.aliasName = aliasName;
	}
	
	public AliasedIndexConfiguration(String aliasName, int shardCount, int replicaCount, int indexCount)
	{
		this(aliasName);
		this.shardCount = shardCount;
		this.replicaCount = replicaCount;
		this.indexCount = indexCount;
	}

	public String getAliasName()
	{
		return aliasName;
	}

	public void setAliasName(String indexName)
	{
		this.aliasName = indexName;
	}

	public int getShardCount()
	{
		return shardCount;
	}

	public void setShardCount(int shardCount)
	{
		this.shardCount = shardCount;
	}

	public int getReplicaCount()
	{
		return replicaCount;
	}

	public void setReplicaCount(int replicaCount)
	{
		this.replicaCount = replicaCount;
	}

	public int getIndexCount()
	{
		return indexCount;
	}

	public void setIndexCount(int indexCount)
	{
		this.indexCount = indexCount;
	}
	
	public Map<String, Integer> getIndexSettings()
	{
		Map<String, Integer> settingsMap = new HashMap<>();
		settingsMap.put("number_of_shards", shardCount);
		settingsMap.put("number_of_replicas", replicaCount);
		return settingsMap;
	}
}
