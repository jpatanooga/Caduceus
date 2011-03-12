package tv.floe.caduceus.hadoop.mapreduce.template;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is a demo job that doenst do much but is intended to demonstrate the structure of a hadoop
 * map reduce job.
 * 
 * This class's main purpose is to "glue" together the job's map and reduce task classes.
 * 
 * This class extends the "Configured" class which allows it to automatically load the proper xml conf files for hadoop.
 * 
 * Note: this example uses the original API; while marked deprecated, it is still in use by many people
 * and at this point I tend to prefer it. Both the new and old MR api's are valid.
 * 
 * @author jpatterson
 *
 */
public class ExampleJob extends Configured implements Tool {
	

	/**
	 * The run method is the method called by the Map Reduce system to construct and drive the job.
	 * 
	 */
	   @Override
	   public int run(String[] args) throws Exception {

		   System.out.println( "\n\nJob\n" );
		   
		   
		   JobConf conf = new JobConf( getConf(), ExampleJob.class );
		   conf.setJobName( "ExampleJob" );
		   
		   conf.setMapOutputKeyClass( Text.class );
		   conf.setMapOutputValueClass( Text.class );

		   conf.setMapperClass( ExampleMapper.class );        
		   conf.setReducerClass( ExampleReducer.class );
		 
		   /*
		    * This section of code is meant to parse command line arguments and load them into 
		    * the distributed configuration system so that all map / reduce tasks can see them. 
		    * 
		    */
		   List<String> other_args = new ArrayList<String>();
		   for(int i=0; i < args.length; ++i) {
		     try {
		       if ("-m".equals(args[i])) {
		       	
		         conf.setNumMapTasks(Integer.parseInt(args[++i]));
		         
		       } else if ("-r".equals(args[i])) {
		       	
		         conf.setNumReduceTasks(Integer.parseInt(args[++i]));
		    	   
		    	   
		    	   		    	   
		       } else {
		       	
		         other_args.add(args[i]);
		         
		       }
		     } catch (NumberFormatException except) {
		       System.out.println("ERROR: Integer expected instead of " + args[i]);
		       return printUsage();
		     } catch (ArrayIndexOutOfBoundsException except) {
		       System.out.println("ERROR: Required parameter missing from " +
		                          args[i-1]);
		       return printUsage();
		     }
		   }
		   // Make sure there are exactly 2 parameters left.
		   if (other_args.size() != 2) {
		     System.out.println("ERROR: Wrong number of parameters: " +
		                        other_args.size() + " instead of 2.");
		     return printUsage();
		   }
		   
		   /*
		    * Here we're setting our input and output formats
		    */
		   conf.setInputFormat( TextInputFormat.class );
		   conf.setOutputFormat(TextOutputFormat.class);
		   conf.setCompressMapOutput(true);	   
		   
		   
		   FileInputFormat.setInputPaths( conf, other_args.get(0) );
		   FileOutputFormat.setOutputPath( conf, new Path(other_args.get(1)) );
		   
		   // and finally we run the job   
		   JobClient.runJob(conf);
		   
		   return 0;
	   }
	   
		 
		
		 static int printUsage() {
		   System.out.println("ExampleJob [-m <maps>] [-r <reduces>] <input> <output>");
		   ToolRunner.printGenericCommandUsage(System.out);
		   return -1;
		 }

	   
		 /**
		  * Method to crank up the ToolRunner app, load the configuration, and drive the job creation method "run()"
		  * 
		  * @param args
		  * @throws Exception
		  */
		 public static void main(String[] args) throws Exception {
		   
			int res = ToolRunner.run( new Configuration(), new ExampleJob(), args );
		    System.exit(res);
		 
		 }
	   	  
	

}
