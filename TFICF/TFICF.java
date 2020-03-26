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
	Path tficfInputPath = dsOutputPath;
	Path tficfOutputPath = new Path("output" + index + "/TFICF");
	
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
	if (hdfs.exists(tficfOutputPath))
	    hdfs.delete(tficfOutputPath, true);
	
	// Create and execute Word Count job
	
	/************ YOUR CODE HERE ************/

	Job job = Job.getInstance(conf, "word count");
	job.setJarByClass(TFICF.class);
	job.setMapperClass(WCMapper.class);
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
	job2.setReducerClass(DSReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job2, dsInputPath);
	FileOutputFormat.setOutputPath(job2, dsOutputPath);
	job2.waitForCompletion(true);
	
	//Create and execute TFICF job
	
	/************ YOUR CODE HERE ************/
	Job job3 = Job.getInstance(conf, "TFICF");
	job3.setJarByClass(TFICF.class);
	job3.setMapperClass(TFICFMapper.class);
	job3.setReducerClass(TFICFReducer.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job3, tficfInputPath);
	FileOutputFormat.setOutputPath(job3, tficfOutputPath);

	//Return final job code , e.g. retrun tficfJob.waitForCompletion(true) ? 0 : 1
	/************ YOUR CODE HERE ************/
	return job3.waitForCompletion(true) ? 0 : 1;
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
        private Text document_text = new Text();
        private Text word_text = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
	    
	    
	    String[] keys = value.toString().split("\\t");
	    String word_doc = keys[0];
	    String word_count = keys[1];
	    String[] doc_key = word_doc.split("@");
	    String current_word = doc_key[0];
	    String document = doc_key[1];
	    document_text.set(document);
	    word_text.set(current_word + "=" + word_count);
	    context.write(document_text, word_text);
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

	    int docSize = 0;
	    List<String> inputs = new ArrayList<String>();
	    for(Text value : values)
		{
		    inputs.add(value.toString());
		    String count = value.toString().split("=")[1];
		    docSize += Integer.parseInt(count);
		    
		}

	    for(String word_input : inputs)
		{
		    String[] word_key = word_input.split("=");
		    String current_word = word_key[0];
		    String current_word_count = word_key[1];
		    Text out_key = new Text(current_word + "@" + key);
		    Text out_val = new Text(current_word_count + "/" + docSize);
		    context.write( out_key, out_val);
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
        private Text word_text = new Text();
        private Text document_text = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
	    
	    
	    String[] keys = value.toString().split("\\t");
	    String word_doc = keys[0];
	    String word_count_per_docSize = keys[1];
	    String[] doc_key = word_doc.split("@");
	    String current_word = doc_key[0];
	    String document = doc_key[1];
	    word_text.set(current_word);
	    document_text.set(document + "=" + word_count_per_docSize);
	    context.write(word_text, document_text);
        }
	
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
	    int numDocsWithWord = 0;
	    List<String> inputs = new ArrayList<String>();
	    for(Text value:values){
		inputs.add(value.toString());
		numDocsWithWord = numDocsWithWord + 1;
	    }

	    for(String word_input : inputs)
		{
		    String[] vals = word_input.split("[=/]");
		    Text out_key = new Text(vals[0]+'@'+key);
		    int numerator = Integer.parseInt(vals[1]);
		    int denominator = Integer.parseInt(vals[2]);
		    double TF = (double)numerator/denominator;
		    int ICF_numerator = numDocs + 1;
		    int ICF_denominator = numDocsWithWord + 1;

		    double tficfValue = Math.log((double)ICF_numerator/ICF_denominator)*Math.log(TF + 1);
		    Text out_val = new Text( String.valueOf(tficfValue) );
		    //Put the output (key,value) pair into the tficfMap instead of doing a context.write
		    tficfMap.put(out_key,out_val);
		}

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
