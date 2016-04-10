package net.uresk.samples.elastic.aliasing;

import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.common.settings.Settings;

public class AliasingRebuildService
{

	private static final String LOCK_DIRECTORY = "locks";
	
	private static final String LOCK_TYPE = "indexing-service-rebuild";
	
	private Client elasticClient;
	
	public AliasingRebuildService(Client elasticClient)
	{
		this.elasticClient = elasticClient;
	}
	
	public <T> IndexOperationResponse<T> rebuildAliasedIndex(AliasedIndexConfiguration config, IndexRebuildJob<T> job)
	{
		String aliasName = config.getAliasName();
		int indexCount = config.getIndexCount();
		
		boolean ok = getLock(aliasName);
		
		if(!ok)
		{
			return new IndexOperationResponse<>(false);
		}
		
		try
		{
			// Figure out which index we actually need to publish to
			
			int currentIndex = findCurrentIndex(aliasName, indexCount);
			int newIndex = -1;
			
			if(currentIndex > -1)
			{
				if(currentIndex + 1 >= indexCount)
				{
					newIndex = 0;
				}
				else
				{
					newIndex = currentIndex + 1;
				}
			}
			else
			{
				newIndex = 0;
			}
			
			String indexName = buildIndexName(aliasName, newIndex);
			
			// Delete it, if it already exists
			
			if(elasticClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists())
			{
				elasticClient.admin().indices().delete(new DeleteIndexRequest(indexName));
			}
			
			// Create the index
			Settings.Builder settingsBuilder = Settings.builder();
			
			for(Map.Entry<String, Integer> setting : config.getIndexSettings().entrySet())
			{
				settingsBuilder.put(setting.getKey(), setting.getValue());
			}
			
			elasticClient.admin().indices().create(new CreateIndexRequest(indexName, settingsBuilder.build()));
			
			// Push the mapping
			
			String mapping = getMapFile(aliasName);
			
			if(mapping != null)
			{
				elasticClient.admin().indices().putMapping(new PutMappingRequest(indexName).source(getMapFile(aliasName)));
			}
			
			// Do the indexing
			T res = job.rebuildIndex(indexName);
			
			// Add/rename alias
			if(currentIndex > -1)
			{
				renameAlias(aliasName, buildIndexName(aliasName, currentIndex), indexName);
			}
			else
			{
				elasticClient.admin().indices().prepareAliases().addAlias(indexName, aliasName).execute();
			}
			
			return new IndexOperationResponse<T>(true, res);
		}
		finally
		{
			releaseLock(aliasName);
		}
	}

	private boolean getLock(String aliasName)
	{
		synchronized (aliasName)
		{
			IndexRequestBuilder indexRequest = elasticClient.prepareIndex(LOCK_DIRECTORY, LOCK_TYPE, aliasName).setSource("{}").setOpType(OpType.CREATE);
			boolean successful = false;
			
			try
			{
				IndexResponse response = indexRequest.get();
				successful = response.isCreated();
			}
			catch(Exception e)
			{
				// This means we couldn't get the lock.
				e.printStackTrace();
			}
			
			return successful;
		}
	}
	
	private void releaseLock(String aliasName)
	{
		synchronized (aliasName)
		{
			DeleteRequest deleteRequest = new DeleteRequest().index(LOCK_DIRECTORY).type(LOCK_TYPE).id(aliasName);
			elasticClient.delete(deleteRequest);
		}
	}
	
	private boolean renameAlias(String aliasName, String oldIndexName, String newIndexName)
	{
		AliasAction delete = new AliasAction(AliasAction.Type.REMOVE, oldIndexName, aliasName);
		AliasAction add = new AliasAction(AliasAction.Type.ADD, newIndexName, aliasName);
		return elasticClient.admin().indices().prepareAliases().addAliasAction(delete).addAliasAction(add).get().isAcknowledged();
	}
	
	private int findCurrentIndex(String aliasName, int indexCount)
	{
		Map<String, AliasOrIndex> allAliases = elasticClient
			.admin()
			.cluster()
			.state(Requests.clusterStateRequest())
			.actionGet()
			.getState()
			.getMetaData()
			.getAliasAndIndexLookup();
		
		AliasOrIndex alias = allAliases.get(aliasName);
		
		if(alias != null)
		{
			if(!alias.isAlias())
			{
				throw new IllegalStateException(String.format("Concrete index with alias name (%s) already exists. Aborting.", aliasName));
			}
			if(alias.getIndices().size() > 1)
			{
				throw new IllegalStateException("Alias has multiple records. Aborting.");
			}
			else
			{
				return getIndexId(alias.getIndices().get(0).getIndex());
			}
		}
		
		return -1;
	}
	
	private String buildIndexName(String aliasName, int id)
	{
		return String.format("%s-%d", aliasName, id);
	}
	
	private int getIndexId(String indexName)
	{
		String[] parts = indexName.split("-");
		return Integer.valueOf(parts[parts.length - 1]);
	}
	
	private String getMapFile(String aliasName)
	{
		InputStream is = AliasingRebuildService.class.getResourceAsStream(String.format("/mappings/%s.json", aliasName));
		
		try
		{
			if(is == null)
			{
				return null;
			}
			
			return IOUtils.toString(is);	
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
}
