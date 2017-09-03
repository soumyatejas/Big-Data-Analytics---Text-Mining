import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLenFreq {

  public static class WordLenMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int tokenLen;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String StrVal = value.toString();
    	String[] tokenVal = StrVal.split(" ");
    	int i=0;
    	int len=tokenVal.length;
    	while (i<len){
    		char[] chars=tokenVal[i].toCharArray();
    		int j=0;
    		for(char c: chars){
    			if(!Character.isLetter(c)){
    				j++;
    			}
    		}
    		if (j==0){
				tokenLen=tokenVal[i].length();
	    		word.set(String.valueOf(tokenLen));
	    		context.write(word, one);
			}
    		i++;
      }
    }
  }

  public static class AddCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordLenFreq.class);
    job.setMapperClass(WordLenMapper.class);
    job.setCombinerClass(AddCountReducer.class);
    job.setReducerClass(AddCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
