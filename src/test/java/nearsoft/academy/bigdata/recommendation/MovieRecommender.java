package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by martin on 9/19/16.
 */

public class MovieRecommender {

    String inputpath;
    Map<String, Integer> mapUser = new HashMap<String, Integer>();
    HashBiMap<String, Integer> mapProduct = HashBiMap.create();
    int total=0;
    UserBasedRecommender recommendation;

    public MovieRecommender (String in) throws TasteException, IOException {
        this.inputpath=in;
        String workingDir = System.getProperty("user.dir");
        String datacsv = workingDir+"/data.csv";
        NewFile(this, datacsv);
        DataModel model = new FileDataModel(new File(datacsv));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        recommendation = new GenericUserBasedRecommender(model, neighborhood, similarity);
    }

    public static void NewFile(MovieRecommender recommender, String datacsv) throws IOException, TasteException {
        File data = new File(datacsv);
        BufferedReader br = null;
        FileWriter fw = new FileWriter(data.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        int iteratorProduct=1;
        int iteratorUser=1;
        StringBuilder line = new StringBuilder();

        try {
            if (!data.exists()) {
                data.createNewFile();
            }
            //Read the .gz file
            FileInputStream fin = new FileInputStream(recommender.inputpath);
            GZIPInputStream gzis = new GZIPInputStream(fin);
            InputStreamReader xover = new InputStreamReader(gzis);
            br = new BufferedReader(xover);
            String sCurrentLine;

            //Create the csv archive
            int productToBe=0;
            int userToBe=0;
            String scoreToBe="";
            while ((sCurrentLine = br.readLine()) != null) {

                if(sCurrentLine.contains("product/productId")){
                    String productId = sCurrentLine.substring(19);
                    if(!recommender.mapProduct.containsKey(productId)){
                        recommender.mapProduct.put(productId, iteratorProduct);
                        productToBe = iteratorProduct;
                        iteratorProduct++;
                    } else{
                        productToBe = recommender.mapProduct.get(productId);
                    }
                }else if(sCurrentLine.contains("review/userId")){
                    String userId=sCurrentLine.substring(15);
                    if(!recommender.mapUser.containsKey(userId)) {
                        recommender.mapUser.put(userId, iteratorUser);
                        userToBe = iteratorUser;
                        iteratorUser++;
                    }else{
                        userToBe = recommender.mapUser.get(userId);
                    }
                }else if(sCurrentLine.contains("review/score")){
                    scoreToBe=sCurrentLine.substring(14);
                    line.append(userToBe).append(",").append(productToBe).append(",").append(scoreToBe).append("\n");
                    bw.write(line.toString());
                    line.delete(0, line.length());
                    recommender.total++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
                bw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public int getTotalReviews() {
        return total;
    }

    public int getTotalProducts() {
        return mapProduct.size();
    }

    public int getTotalUsers() {
        return mapUser.size();
    }

    public List<String> getRecommendationsForUser(String user) throws TasteException {
        int realUser=mapUser.get(user);
        List<RecommendedItem> recommendations = recommendation.recommend(realUser, 5);
        List<String> realrecommendations=new ArrayList<String>();

        for (RecommendedItem recommendation : recommendations) {
            BiMap<Integer, String> BimapProduct = mapProduct.inverse();
            long itemID = recommendation.getItemID();
            int itemIDInt = (int) itemID;
            String product = BimapProduct.get(itemIDInt);
            realrecommendations.add(product);
        }
        return realrecommendations;
    }
}
