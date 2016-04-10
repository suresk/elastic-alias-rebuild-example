package net.uresk.samples.elastic.aliasing.sample;

import java.net.InetAddress;
import java.util.List;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import net.uresk.samples.elastic.aliasing.AliasedIndexConfiguration;
import net.uresk.samples.elastic.aliasing.AliasingRebuildService;

public class AliasRebuildApp
{

	private static String nodeList = "localhost:9300";
	
	public static void main(String[] args) throws Exception
	{
		
		TransportClient client = TransportClient.builder().build();
		
		String[] nodes = nodeList.split(",");
		
		for(String node : nodes)
		{
			String[] parts = node.split(":");
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.valueOf(parts[1])));
		}
		
		AliasingRebuildService service = new AliasingRebuildService(client);
		
		AliasedIndexConfiguration config = new AliasedIndexConfiguration("cars", 2, 0, 2);
		CarIndexRebuildJob job = new CarIndexRebuildJob(client, "car");
		List<String> results = service.rebuildAliasedIndex(config, job).getResult();
		
		for(String result : results)
		{
			System.out.println("Car Id: " + result);
		}
	}

}
