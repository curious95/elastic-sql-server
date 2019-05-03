package com.dev.elastic;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MultiMatchQuery;
import org.elasticsearch.index.search.MultiMatchQuery.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Hello world!
 *
 */
public class App {
	
	public static SearchResponse searchElastic() throws IOException {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("192.168.0.100", 9200, "http")));

		SearchRequest searchRequest = new SearchRequest("yachtdb");

		
		
		MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name", "MY AKULA"); 
		matchQueryBuilder.fuzziness(Fuzziness.AUTO);
		matchQueryBuilder.prefixLength(0); 
		matchQueryBuilder.maxExpansions(100);
		
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.size(10);
		searchSourceBuilder.query(matchQueryBuilder);
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse response=null;
		try {
			response = client.search(searchRequest,RequestOptions.DEFAULT);
			System.out.println(response.status());
			
			SearchHit[] searchHits = response.getHits().getHits();
			for (SearchHit hit : searchHits) {
			    // do something with the SearchHit
				
				
				JsonParser jsonParser = new JsonParser();
				JsonElement jsonTree = jsonParser.parse(hit.getSourceAsString());
				
				//System.out.println(hit.getScore()+"  "+jsonTree.getAsJsonObject().get("builder").getAsString());
				

			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	


		client.close();
		
		return response;
	}
}
