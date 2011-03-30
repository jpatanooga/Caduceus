package tv.floe.caduceus.mahout.cf.taste.samples;

import java.io.*;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class SampleRecommender {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws TasteException 
	 */
	public static void main(String[] args) throws IOException, TasteException {
		
		DataModel model = new FileDataModel( new File( "data/mahout/cf/sample_recommender_data.csv" ) ); // load model
		
		UserSimilarity similarity = new PearsonCorrelationSimilarity( model );
		
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model );

		Recommender recommender = new GenericUserBasedRecommender( model, neighborhood, similarity );
		
		List<RecommendedItem> recommendations = recommender.recommend(1, 1);
		
		for ( RecommendedItem recommendation : recommendations ) {
			
			System.out.println( recommendation );
			
		}
		
		
	}

}
