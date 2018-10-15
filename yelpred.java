package yelpavg;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;

public class yelpred extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	double count = 0,sum_rating=0,avg_rating=0,
    			sum_stars=0,avg_stars=0;
        for(Text value: values){
        	String line = value.toString();
   	        String arryUsers[] = line.split(",");
        	Double rating = Double.parseDouble(arryUsers[0].trim()); 
        	Double stars = Double.parseDouble(arryUsers[1].trim()); 
        	
        	sum_stars += stars;
        	sum_rating += rating;
        	
            count += 1;
        }
        
        avg_rating = (double) Math.round((sum_rating/count) * 10)/10;
        avg_stars = (double) Math.round((sum_stars/count) * 10)/10;
        
        output.write(new Text(key + ",") ,new Text(String.valueOf(avg_rating)  + "," + String.valueOf(avg_stars)));
    }
}

