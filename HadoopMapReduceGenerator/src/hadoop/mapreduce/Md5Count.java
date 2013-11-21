package hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Md5Count
{
    public static class WordMapper extends Mapper<Text, Text, Text, Text>
    {
    	 private Text word = new Text();
        private Text word2 = new Text();
        
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
        	
        	//word2.set("Hello");
        	//context.write(key,word2);
        	
        	StringTokenizer itr = new StringTokenizer(key.toString(),",");
        	//word.set(itr.nextToken());
        	//word2.set(itr.nextToken());
        	//context.write(word,word2);
        	StringBuffer sb =new StringBuffer();
            int i=0;
            while (itr.hasMoreTokens()) {
          	  String t=(String)itr.nextElement();
          	  if(i==0){
          		sb.append(t);
          		i++;
          	  }
          	  else if(i==1){
          		//sb.append(",");
          		//sb.append(t);
          		i++;
          	  }
          	  else if(i==2){
          		  word.set(t);
          		  word2.set(sb.toString());
          	      context.write(word, word2);
          	      i=0;
          	  } 
              
            }
        }
    }
    public static class AllTranslationsReducer
    extends Reducer<Text,Text,Text,Text>
    {
        private Text result = new Text();
        private int count=0;
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException
        {
            String translations = "";
            
            for (Text val : values)
            {
                translations += "|"+val.toString();
                count++;
            }
            result.set(count+","+translations);
            count=0;
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "md5count");
        job.setJarByClass(Md5Count.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(AllTranslationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
//change here
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

