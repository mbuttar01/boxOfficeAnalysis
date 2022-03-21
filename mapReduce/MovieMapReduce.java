import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

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

public class MovieMapReduce {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    private Text genreKey = new Text();
    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n"); //whole line
	  //split uses regular expressions
	  
            while (itr.hasMoreTokens())
            {
                String imdbTitle, title, year, genres, budget, income, remainingLine;
                String[] temp;
                String currLine;// = new Text();
                currLine = itr.nextToken();

                //imdbTitle
                temp = currLine.split(",\"",2);
                imdbTitle = temp[0];
                remainingLine = temp[1];

                //title
                temp = remainingLine.split("\",",2);
                title = temp[0];
                remainingLine = temp[1];

                //year
                temp = remainingLine.split(",\"",2);
                year = temp[0];
                remainingLine = temp[1];

                //genre
                temp = remainingLine.split("\",",2);
                genres = temp[0];
                remainingLine = temp[1];

                //budget and income
                temp = remainingLine.split(",",2);
                budget = temp[0];
                income = temp[1];

                // -----------------------------------------------------------------------
                // | At this point, the initial line has been broken into its components |
                // -----------------------------------------------------------------------

                double budgetDouble = Double.parseDouble(budget);
                double incomeDouble = Double.parseDouble(income);

                //set ratio
                double ratio = (incomeDouble / budgetDouble);

                // Get year as an integer for filtering by decade
                int yearInt = Integer.parseInt(year);

                //set output value as "Title::Ratio"
                // This will be split in the reducer
                String ratioString = Double.toString(ratio);
                outputValue.set(title + "_" + ratioString);

                // At this point, we need to determine what decade the film belongs to.
                // For our analysis, we only want decades from 1960 onward for decade-specific analysis.
                String decade = "";
                boolean isValid = false;
                if (yearInt < 1960)
                {
                    isValid = false;
                }
                else if (yearInt < 1970)
                {
                    isValid = true;
                    decade = ":1960s";
                }
                else if (yearInt < 1980)
                {
                    isValid = true;
                    decade = ":1970s";
                }
                else if (yearInt < 1990)
                {
                    isValid = true;
                    decade = ":1980s";
                }
                else if (yearInt < 2000)
                {
                    isValid = true;
                    decade = ":1990s";
                }
                else if (yearInt < 2010)
                {
                    isValid = true;
                    decade = ":2000s";
                }
                else // (yearInt <= 2020)
                {
                    isValid = true;
                    decade = ":2010s";
                }

                if (isValid)
                {
                    // Determine the base genreKey
                    String[] genreArray = genres.split(", ");
                    Arrays.sort(genreArray);
                    
                    String genreKeyTemp = ""; // + decade;
                    // Determine the complex (i.e. multi-) genre key			
                    for(int i = 0; i < genreArray.length; i++){
                        genreKeyTemp = genreKeyTemp + genreArray[i];
                        if(i + 1 < genreArray.length){
                            genreKeyTemp = genreKeyTemp + ":";
                        }
                    }
		    genreKeyTemp = genreKeyTemp + decade;
                    genreKey.set(genreKeyTemp);
                    context.write(genreKey, outputValue);
                    // At this point, we want to also write the information to each individual genre that composes the complex genre key
                    if(genreArray.length > 1) // We only need to do that if it was, in fact, a multi-genre key
                    {
                        for (int i = 0; i < genreArray.length; i++)
                        {
                                genreKeyTemp = genreArray[i] + decade;
                                genreKey.set(genreKeyTemp);
                                context.write(genreKey, outputValue);
                        }
                    }
                
                }
                
            // End While Loop       
            }        
    }
}

  public static class WrappedReducer extends Reducer<Text,Text,Text,Text> {
	
    private Text result1 = new Text();
    private Text result2 = new Text();
    private Text result3 = new Text();
    private Text result4 = new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        String currentEntry;
        String entryTitle;
        
        String[] top2 = new String[2];
        String[] bottom2 = new String[2];
        
        double[] top2Ratios = new double[2];
        double[] bottom2Ratios = new double[2];
        top2Ratios[0] = 0;
        top2Ratios[1] = 0;
        bottom2Ratios[0] = 100;
        bottom2Ratios[1] = 100;
        
        double entryRatio;
        double avgRatio = 0; // This will initially be a sum of all the ratios, then be divided by the total count
        
        double countSuccessful = 0; // This is a count of how many films had (ratio > 1)
        int countTotal = 0; // This is a count of how many films there were.
        
        for (Text val : values) {
            currentEntry = val.toString();
            String[] temp = currentEntry.split("_",2);
            entryTitle = temp[0];
            entryRatio = Double.parseDouble(temp[temp.length-1]);


            avgRatio += entryRatio;

            countTotal++;
            if (entryRatio > 1)  
            {
                countSuccessful++;
            }

            if (entryRatio > top2Ratios[1])
            {
                if (entryRatio > top2Ratios[0])
                {
                    top2Ratios[1] = top2Ratios[0];
                    top2[1] = top2[0];

                    top2Ratios[0] = entryRatio;
                    top2[0] = entryTitle;
                }
                else
                {
                    top2Ratios[1] = entryRatio;
                    top2[1] = entryTitle;
                }
            }
            else if (entryRatio < bottom2Ratios[1])
            {
                if (entryRatio < bottom2Ratios[0])
                {
                    bottom2Ratios[1] = bottom2Ratios[0];
                    bottom2[1] = bottom2[0];

                    bottom2Ratios[0] = entryRatio;
                    bottom2[0] = entryTitle;
                }
                else
                {
                    bottom2Ratios[1] = entryRatio;
                    bottom2[1] = entryTitle;
                }
            }   
        }
 
        Text key2 = new Text();
        
        String keyTemp = key.toString();
        
        // 1.
        avgRatio = avgRatio / countTotal;
        result1.set(Double.toString(avgRatio));
	String key1 = key.toString() + ":avgRatio:";
	key2.set(key1);
        context.write(key2, result1);
        
        // 2.
        double percentSuccessful = countSuccessful/countTotal;

        key2.set(keyTemp + ":SuccessPercent:");
        result2.set(Double.toString(percentSuccessful));
        context.write(key2, result2);
        
        // 4.
        key2.set(keyTemp + ":Representation:");
        result3.set(Integer.toString(countTotal));
        context.write(key2, result3);
        
        // 5.
        key2.set(keyTemp + ":Top2:");
        result4.set(top2[0] + "::Ratio=" + Double.toString(top2Ratios[0]) + " && " + top2[1] + "::Ratio=" + Double.toString(top2Ratios[1]) );
        context.write(key2, result4);
        
        key2.set(keyTemp + ":Bottom2:");
        result4.set(bottom2[0] + "::Ratio=" + Double.toString(bottom2Ratios[0]) + " && " + bottom2[1] + "::Ratio=" + Double.toString(bottom2Ratios[1]) );
        context.write(key2, result4);
    }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MovieMapReduce");
    job.setJarByClass(MovieMapReduce.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(WrappedReducer.class);
    job.setReducerClass(WrappedReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
