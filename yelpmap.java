package yelpavg;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import yelpavg.Votes;
import yelpavg.Review;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStream;   
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.io.FileInputStream;
import org.apache.hadoop.io.DoubleWritable;
import com.google.gson.Gson;

public class yelpmap extends Mapper<Object, Text, Text, Text>{
	
	private HashMap<String, Double> map = new HashMap<String, Double>();
	
	@Override
	public void setup(Context context) throws IOException,
			InterruptedException {
		try {
			Path[] files = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			if (files == null || files.length == 0) {
				throw new RuntimeException(
						"User information is not set in DistributedCache");
			}

			// Read all files in the DistributedCache
			BufferedReader rdr = new BufferedReader(
						new InputStreamReader(
								new FileInputStream(
										new File(files[0].toString()))));

				String line;
				// For each record in the word file
				while ((line = rdr.readLine()) != null) {

					        String type = "";
        					String word = "";
        					String polarity = "";

        					String[] properties = line.split(" ");
        					for (String property : properties) {

            				if (!property.contains("=")) {
                					continue;
            					}

            				String[] keyVal = property.split("=");
            				String key = keyVal[0];
            				String val = keyVal[1];

            				if (key.equals("type")) {
                					type = val;
            				} else if (key.equals("word1")) {
               					 word = val;
            				} else if (key.equals("priorpolarity")) {
                					polarity = val;
            				}		
															
					       double value;
        					if (type.equals("strongsubj")) {
            					value = 1;
        					} else {
            					value = 1;
        					}
        					
        					if (polarity.equals("negative")) {
                				value = -value;
            					}
        					
        					map.put(word, value);	
        					}			
				
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}   
    

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                
    	      
    	       Gson gson = new Gson();
    	       if (!value.toString().contains("\"text\"")) {
    	            return; 
    	        }
    	                     	             	
    	        Review emp = gson.fromJson(value.toString(), Review.class);
            	
            	double sum = 0,pos_sum = 0 ,neg_sum=0;
            	double rating=0;
            	String text1 = emp.getText().replaceAll("[\"#$%^&*@\\-=:;?().,]", "").replaceAll("\n"," ");
            	String text2 = text1.toLowerCase();
            	StringTokenizer itr = new StringTokenizer(text2);
            		    while (itr.hasMoreTokens()) {
            		    	String temp = itr.nextToken();
            		    	if (map.containsKey(temp)) {
            		    		if (map.get(temp) > 0) { pos_sum = pos_sum + map.get(temp);}
            		    		if (map.get(temp) < 0) {neg_sum = neg_sum + map.get(temp);}
            		    	}
            		    }
            		    
            	if (pos_sum >= 2 && neg_sum == 0) { rating = 5;}
            	else if (pos_sum >=2 && neg_sum == -1) {rating =4;}
            	else if (pos_sum >=2 && neg_sum <= -2) {rating =3;}
            	else if (pos_sum ==1 && neg_sum == -1) {rating =3;}
            	else if (pos_sum ==1 && neg_sum == 0) {rating =4;}
            	else if (pos_sum ==1 && neg_sum <= -2) {rating =2;}
            	else if (pos_sum ==0 && neg_sum == 0) {rating =3;}
            	else if (pos_sum ==0 && neg_sum == -1) {rating =2;}
            	else if (pos_sum ==0 && neg_sum <=-2) {rating =1;}
            	    
            	
            	String business_id1 = emp.getBusiness_id();
            	String stars = emp.getStars();
            	            	
            	context.write(new Text( business_id1), new Text(String.valueOf(rating)  + "," + new Text(stars)));
            	
                           
        	
        }
    }


