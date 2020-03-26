import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/*
 * Main class of the TFICF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFICF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 2) {
            System.err.println("Usage: TFICF <input corpus0 dir> <input corpus1 dir>");
            System.exit(1);
        }
	
	// return value of run func
	int ret = 0;
	
	// Create configuration
	Configuration conf0 = new Configuration();
	Configuration conf1 = new Configuration();
	
	// Input and output paths for each job
	Path inputPath0 = new Path(args[0]);
	Path inputPath1 = new Path(args[1]);
        try{
            ret = run(conf0, inputPath0, 0);
        }catch(Exception e){
            e.printStackTrace();
        }
        if(ret == 0){
	    try{
		run(conf1, inputPath1, 1);
	    }catch(Exception e){
		e.printStackTrace();
	    }        
        }
     
	System.exit(ret);
    }
    
    public static int run(Configuration conf, Path path, int index) throws Exception{
	// Input and output paths for each job

	Path wcInputPath = path;
	Path wcOutputPath = new Path("output" +index + "/WordCount");
	Path dsInputPath = wcOutputPath;
	Path dsOutputPath = new Path("output" + index + "/DocSize");
	/*Path tficfInputPath = dsOutputPath;
	  Path tficfOutputPath = new Path("output" + index + "/TFICF");*/
	
	// Get/set the number of documents (to be used in the TFICF MapReduce job)
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(path);
	String numDocs = String.valueOf(stat.length);
	conf.set("numDocs", numDocs);
	
	// Delete output paths if they exist
	FileSystem hdfs = FileSystem.get(conf);
	if (hdfs.exists(wcOutputPath))
	    hdfs.delete(wcOutputPath, true);
	if (hdfs.exists(dsOutputPath))
	    hdfs.delete(dsOutputPath, true);
	/*iif (hdfs.exists(tficfOutputPath))
	  hdfs.delete(tficfOutputPath, true);*/
	
	// Create and execute Word Count job
	
	/************ YOUR CODE HERE ************/

	Job job = Job.getInstance(conf, "word count");
	job.setJarByClass(TFICF.class);
	job.setMapperClass(WCMapper.class);
	//job.setCombinerClass(WCReducer.class);
	job.setReducerClass(WCReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, wcInputPath);
	FileOutputFormat.setOutputPath(job, wcOutputPath);
	job.waitForCompletion(true);
	// Create and execute Document Size job
	
	/************ YOUR CODE HERE ************/
	Job job2 = Job.getInstance(conf, "document size");
	job2.setJarByClass(TFICF.class);
	job2.setMapperClass(DSMapper.class);
	//job2.setCombinerClass(DSReducer.class);
	job2.setReducerClass(DSReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job2, dsInputPath);
	FileOutputFormat.setOutputPath(job2, dsOutputPath);
	job2.waitForCompletion(true);
	return 0;
	
	//Create and execute TFICF job
	
	/************ YOUR CODE HERE ************/

	//Return final job code , e.g. retrun tficfJob.waitForCompletion(true) ? 0 : 1
	/************ YOUR CODE HERE ************/
	
    }
    
    /*
     * Creates a (key,value) pair for every word in the document 
     *
     * Input:  ( byte offset , contents of one line )
     * Output: ( (word@document) , 1 )
     *
     * word = an individual word in the document
     * document = the filename of the document
     */
    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

        /************ YOUR CODE HERE ************/
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            while (itr.hasMoreTokens()) {
                String current_word = itr.nextToken();
                current_word = current_word.toLowerCase().trim();
		current_word = current_word.replaceAll("[^a-zA-Z0-9-\\[\\]`]", "");

                if (!current_word.isEmpty())
                    {
			if ( current_word.charAt(0) >= 'a' && current_word.charAt(0) <= 'z' )
			    {
				word.set(current_word+"@"+fileName);
				context.write(word, one);
			    }
                    }
            }
        }


    }

    /*
     * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
     *
     * Input:  ( (word@document) , 1 )
     * Output: ( (word@document) , wordCount )
     *
     * wordCount = number of times word appears in document
     */
    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	/************ YOUR CODE HERE ************/
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,Context context
			   ) throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable val : values) {
		sum += val.get();
	    }
	    result.set(sum);
	    context.write(key, result);
	}
	
    }
    
    /*
     * Rearranges the (key,value) pairs to have only the document as the key
     *
     * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	      */
    public static class DSMapper extends Mapper<Object, Text, Text, Text> {
	
	/************ YOUR CODE HERE ************/
	private Text document = new Text();
	private Text result = new Text();

        public void map(Object key, Text line, Context context
                        ) throws IOException, InterruptedException {
	    
	    
	    String s = line.toString();
	    document.set(s.split("\t")[0].trim().split("@")[1]);
	    result.set(s.split("\t")[0].trim().split("@")[0].concat("=").concat(s.split("\t")[1].trim()));
	    context.write(document, result);
	}
    
    
}

/*
 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
 *
 * Input:  ( document , (word=wordCount) )
 * Output: ( (word@document) , (wordCount/docSize) )
 *
 * docSize = total number of words in the document
 */
public static class DSReducer extends Reducer<Text, Text, Text, Text> {
    
    /************ YOUR CODE HERE ************/

    public void reduce(Text key, Iterable<Text> values,Context context
		       ) throws IOException, InterruptedException {

	int sum = 0;
	int size = 0;

	// Keep a cache of elements, so we can iterate again
	ArrayList<Text> cache = new ArrayList<Text>();

	for (Text val: values) {
	    Text tmp = new Text();
	    tmp.set(val);
	    sum += Integer.parseInt(val.toString().split("=")[1]);
	    size += 1;
	    cache.add(tmp);
	}
	
	for (int i=0; i<size; i++) {
	    Text word = new Text();
	    Text result = new Text(); 
	    Text val = new Text();
	    val = cache.get(i);
	    word.set(val.toString().split("=")[0].trim().concat("@").concat(key.toString()));
	    result.set(val.toString().split("=")[1].trim().concat("/").concat(Integer.toString(sum)));
	    context.write(word, result);
	}

    }
    
}

/*
 * Rearranges the (key,value) pairs to have only the word as the key
 * 
 * Input:  ( (word@document) , (wordCount/docSize) )
 * Output: ( word , (document=wordCount/docSize) )
 */
public static class TFICFMapper extends Mapper<Object, Text, Text, Text> {

    /************ YOUR CODE HERE ************/
    
}

/*
 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
 * the final TFICF value (TFICF). Along the way, calculates the total number of documents and 
 * the number of documents that contain the word.
 * 
 * Input:  ( word , (document=wordCount/docSize) )
 * Output: ( (document@word) , TFICF )
 *
 * numDocs = total number of documents
 * numDocsWithWord = number of documents containing word
 * TFICF = ln(wordCount/docSize + 1) * ln(numDocs/numDocsWithWord +1)
 *
 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
 *       is highly inefficient!
 */
public static class TFICFReducer extends Reducer<Text, Text, Text, Text> {
    
    private static int numDocs;
    private Map<Text, Text> tficfMap = new HashMap<>();
    
    // gets the numDocs value and stores it
    protected void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	numDocs = Integer.parseInt(conf.get("numDocs"));
    }
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
	/************ YOUR CODE HERE ************/
	 
	//Put the output (key,value) pair into the tficfMap instead of doing a context.write
	//tficfMap.put(/*document@word*/, /*TFICF*/);
    }
    
    // sorts the output (key,value) pairs that are contained in the tficfMap
    protected void cleanup(Context context) throws IOException, InterruptedException {
	Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tficfMap);
	for (Text key : sortedMap.keySet()) {
	    context.write(key, sortedMap.get(key));
	}
    }
    
}
}
