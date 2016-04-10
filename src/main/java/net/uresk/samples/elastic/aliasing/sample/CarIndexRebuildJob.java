package net.uresk.samples.elastic.aliasing.sample;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;

import net.uresk.samples.elastic.aliasing.IndexRebuildJob;

public class CarIndexRebuildJob implements IndexRebuildJob<List<String>>
{

	private Client elasticClient;
	
	private String type;
	
	public CarIndexRebuildJob(Client elasticClient, String type)
	{
		this.elasticClient = elasticClient;
		this.type = type;
	}
	
	@Override
	public List<String> rebuildIndex(String indexName)
	{
		String audiS4 = indexCar(indexName, "Audi", "S4", 2015);
		String mercedesE63 = indexCar(indexName, "Mercedes", "E63 AMG", 2016);
		String lambo = indexCar(indexName, "Lamborghini", "Huracan", 2015);
		return Arrays.asList(audiS4, mercedesE63, lambo);
	}
	
	private String indexCar(String indexName, String make, String model, Integer year)
	{
		return elasticClient.prepareIndex(indexName, type).setSource(buildCar(make, model, year)).get().getId();
	}
	
	private Map<String, Object> buildCar(String make, String model, Integer year)
	{
		Map<String, Object> car = new HashMap<>();
		
		car.put("make", make);
		car.put("model", model);
		car.put("year", year);
		
		return car;
	}

}
