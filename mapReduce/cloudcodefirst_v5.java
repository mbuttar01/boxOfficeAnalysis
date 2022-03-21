
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class cloudcodefirst {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private DoubleWritable ratio = new DoubleWritable();
    private Text genreKey = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); //whole line
	  //split uses regular expressions
	  


		while (itr.hasMoreTokens()){
			String imdbTitle, title, year, genre, budget, income, remainingLine;

			String[] temp;
			String[] genreees;
			String currLine = "";
			currLine = itr.nextToken();

			//imdbTitle
			//getline(infile1, imdbTitle, ',');
			temp = currLine.split(",\"",2);
			imdbTitle = temp[0];
			remainingLine = temp[1];

			//title
			temp = remainingLine.split("\",",2);
			//getline(infile1, title, ',');
			title = temp[0];
			remainingLine = temp[1];

			//year
			temp = remainingLine.split(",\"",2);
			year = temp[0];
			remainingLine = temp[1];
			//getline(infile1, year, ',');
		
			//genre
			temp = remainingLine.split("\",",2);
			genre = temp[0];
			remainingLine = temp[1];
			//getline(infile1, genre, ',');
			
			//budget and income
			temp = remainingLine.split(",",2);
			budget = temp[0];
			income = temp[1];
			
			double budgetDouble = Double.parseDouble(budget);
			double incomeDouble = Double.parseDouble(income);
			
			
			ratio.set(incomeDouble/budgetDouble);

			genreees = genre.split(", ");
			Arrays.sort(genreees);
            
            String genreeetemp = "";
			//change data structure for efficiency later			
            for(int i = 0; i < genreees.length; i++){
                genreeetemp = genreeetemp + genreees[i];
                if(i + 1 < genreees.length){
                    genreeetemp = genreeetemp + ":";
                }
            }
			
			//sets ratio to total genre pairing as well as each individual genre
			
			genreKey.set(genreeetemp);
			context.write(genreKey, ratio);

			if(genreees.length > 1){
				for(int i = 0; i < genreees.length; i++){
					genreeetemp = genreees[i];
					genreKey.set(genreeetemp);
					context.write(genreKey, ratio);

				}
			}
		}
	}
}


  public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
    private DoubleWritable result = new DoubleWritable();
    private DoubleWritable Output3 = new DoubleWritable();
    private DoubleWritable Output4 = new DoubleWritable();
    //private DoubleWritable meanOutput = new DoubleWritable();
    //private DoubleWritable varianceOutput = new DoubleWritable();
    private DoubleWritable standardDeviationOutput = new DoubleWritable();
    private DoubleWritable perfUpper = new DoubleWritable();
    private DoubleWritable perfLower = new DoubleWritable();
    private DoubleWritable percentAboveThreshold = new DoubleWritable();
    private DoubleWritable percentBelowThreshold = new DoubleWritable();
    private DoubleWritable numOutlierAboveThreshold = new DoubleWritable();
    private DoubleWritable numOutlierBelowThreshold = new DoubleWritable();
    private DoubleWritable Output5 = new DoubleWritable();
    private DoubleWritable Output6 = new DoubleWritable();
    private DoubleWritable outlierUpper = new DoubleWritable();
    private DoubleWritable outlierLower = new DoubleWritable();


    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      

   
      double sum = 0.0;
      double mean = 0.0;
      double ratio = 0.0;
      double posPercentage = 0.0;
      double negPercentage = 0.0;
      double mostPositive = 0.0;
      double mostNegative = 0.0;
      double runningTotal = 0.0;
      double printRatios = 0.0;
      double median = 0.0;
      double variance = 0.0;
      double standardDeviation = 0.0;
      double upperLimit =0.0;
      double lowerLimit = 0.0;
      double outlierlimit = 0.0;


      double sqErrorDiff = 0.0;
     ArrayList<Double> arrayOfRatios = new ArrayList<Double>();



      for (DoubleWritable val : values) {

      	runningTotal++;


        ratio = val.get();

      	arrayOfRatios.add(ratio); 

      	sum += ratio;

         if(ratio > 1 ){
         	mostPositive += 1;

         }
         else if (ratio < 1){
         	mostNegative += 1;
         }


      }

      mean = sum/runningTotal;
      //meanOutput.set(mean);

      for(Double j : arrayOfRatios){
      		sqErrorDiff += (j - mean) * (j-mean);
      }

      variance = sqErrorDiff/runningTotal;
      //varianceOutput.set(variance);

      standardDeviation = Math.sqrt(variance);
      standardDeviationOutput.set(standardDeviation);
      upperLimit = (mean + (2 * standardDeviation));
      lowerLimit = (mean - (2 * standardDeviation));
      int counter1 = 0;
      int counter2 = 0;

      for(Double k : arrayOfRatios){

        if(k > upperLimit){
            counter1++;
        }
        if(k < lowerLimit){
            counter2++;
        }
          
      }
      
      double percentageOverThreshold = ((double)counter1/runningTotal) * 100;
      double percentageUnderThreshold = ((double)counter2/runningTotal) * 100;
      
      percentAboveThreshold.set(percentageOverThreshold);
      percentBelowThreshold.set(percentageUnderThreshold);
      numOutlierAboveThreshold.set(counter1);
      numOutlierBelowThreshold.set(counter2);
      outlierUpper.set(upperLimit);
      outlierLower.set(lowerLimit);
      
      Text newKey = new Text();
      newKey.set(key.toString() + ":%AboveThresh");
      context.write(newKey, percentAboveThreshold);
      newKey.set(key.toString() + ":%BelowThresh");
      context.write(newKey, percentBelowThreshold);
      newKey.set(key.toString() + ":#OutlierAbove");
      context.write(newKey, numOutlierAboveThreshold);
      newKey.set(key.toString() + ":#OutlierBelow");
      context.write(newKey, numOutlierBelowThreshold);
      newKey.set(key.toString() + ":OutlierUpperLimit");
      context.write(newKey, outlierUpper);
      newKey.set(key.toString() + ":OutlierLowerLimit");
      context.write(newKey, outlierLower);
      newKey.set(key.toString() + ":StdDeviation");
      context.write(newKey, standardDeviationOutput);
      //context.write(meanOutput);
      //context.write(varianceOutput);
      
      
 //     for(int i : arrayOfRatios){
 //     	Output4 = i;
 //     	context.write(Output4); 
 //     }
      
      Collections.sort(arrayOfRatios);
      median = arrayOfRatios.get((int)Math.floor((arrayOfRatios.size()/2)));
      Output3.set(median);
      newKey.set(key.toString() + ":Median");
      context.write(newKey, Output3);

      posPercentage = (mostPositive/runningTotal) * 100;
      Output5.set(posPercentage);
      newKey.set(key.toString() + ":mostPos");
      context.write(newKey, Output5);

      negPercentage = (mostNegative/runningTotal) * 100;
      Output6.set(negPercentage);
      newKey.set(key.toString() + ":mostNeg");
      context.write(newKey, Output6);
     
    }
  }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cloudcodefirst");
    job.setJarByClass(cloudcodefirst.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

