package com.dev.elastic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import me.xdrop.fuzzywuzzy.FuzzySearch;

public class Runner {

	public static void main(String[] args) {

		DataBaseFetcer df = new DataBaseFetcer();
		List<Object[]> records=new ArrayList<Object[]>();
		for (int i = 1000; i < 114960; i += 1000) {

			System.out.println("Offset : "+i);
			
			try {
				records  = df.getDB(i);
				
				for(Object[] ob:records) {
				
					String id  = ob[0].toString();
					String name  = ob[1].toString();
					String year  = ob[4]+"";
					String builder  = ob[6].toString();
					//System.out.println("DataBase Record  "+ob[0]+"  "+ob[1]+"   "+ob[4]+"  "+ob[6]);
					Runner.match(id, name, year, builder);
				}

				
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

		}

	}
	
	public static void match(String id, String name, String year, String builder) {
		App ap = new App();
		try {
			SearchResponse response = ap.searchElastic(name);
			SearchHit[] searchHits = response.getHits().getHits();
			JsonParser jsonParser = new JsonParser();
			JsonElement jsonTree = jsonParser.parse(searchHits[0].getSourceAsString());
			
			int nameval = FuzzySearch.ratio(name, jsonTree.getAsJsonObject().get("name").getAsString());
			//int yearVal = FuzzySearch.ratio(year, jsonTree.getAsJsonObject().get("year").getAsString());
			//int builderVal = FuzzySearch.ratio(builder, jsonTree.getAsJsonObject().get("builder").getAsString());
			
			if(nameval > 90) {
				System.out.println("DataBase Record  "+id+"  "+name+"   "+year+"  "+builder);

				System.out.println(searchHits[0].getScore()+"  "+jsonTree.getAsJsonObject().get("name").getAsString()+"   "+jsonTree.getAsJsonObject().get("builder").getAsString()+"  "+jsonTree.getAsJsonObject().get("year").getAsString());

				System.out.println(searchHits[0].getId());
				
				Map<String, Object> jsonMap = new HashMap<>();
				
				jsonMap.put("source_id", id);
				jsonMap.put("destination_id", searchHits[0].getId());
				jsonMap.put("name", jsonTree.getAsJsonObject().get("name").getAsString());
				jsonMap.put("year", jsonTree.getAsJsonObject().get("year").getAsString());
				jsonMap.put("builder", jsonTree.getAsJsonObject().get("builder").getAsString());
				jsonMap.put("source", "manual_crew");
				
				Runner.pushElastic(jsonMap);

				
			}
			
			//System.err.println();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}
	
	public static  void pushElastic(Map<String, Object> jsonMap) {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("192.168.0.100", 9200, "http")));
		
		IndexRequest indexRequest = new IndexRequest("matchedyachts", "doc")
		        .source(jsonMap);
		
		try {
			client.index(indexRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
