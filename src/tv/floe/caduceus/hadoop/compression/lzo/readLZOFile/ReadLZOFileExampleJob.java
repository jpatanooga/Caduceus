package tv.floe.caduceus.hadoop.compression.lzo.readLZOFile;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.log4j.Logger;

import com.hadoop.mapred.DeprecatedLzoTextInputFormat;

public class ReadLZOFileExampleJob extends Configured implements Tool
{

	   private static final Logger logger = Logger.getLogger(ReadLZOFileExampleJob.class);

	   @Override
	   public int run(String[] args) throws Exception {

		   System.out.println( "\n\nRunning Read LZO File Example Job\n" );
		   
		   
		   JobConf conf = new JobConf( getConf(), ReadLZOFileExampleJob.class );
		   conf.setJobName( "ReadLZOFileExampleJob" );
		   
		   conf.setMapOutputKeyClass( Text.class );
		   conf.setMapOutputValueClass( Text.class );

		   conf.setMapperClass( ReadLZOFileMapper.class );        
		   conf.setReducerClass( ReadLZOFileReducer.class );
		 
	   
		   boolean bLZO = false;
		   boolean bCompressOutput = false;
		   
		   List<String> other_args = new ArrayList<String>();
		   for(int i=0; i < args.length; ++i) {
		     try {
		       if ("-m".equals(args[i])) {
		       	
		         conf.setNumMapTasks(Integer.parseInt(args[++i]));
		         
		       } else if ("-r".equals(args[i])) {
		       	
		         conf.setNumReduceTasks(Integer.parseInt(args[++i]));
			       	
			       	
		       } else if ("-lzo_input".equals(args[i])) {
		         
		    	   bLZO = true;
		    	   
		       } else if ( "-lzo_output".equals( args[i] ) ) {
		    	   
		    	   bCompressOutput = true;
		    	   
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
		   
		   if (bLZO) {
			   
			   System.out.println( "\n\nUsing LzoTextInputFormat\n" );
			   
			   // when we switch over to LZO compressed formats, will need hadoop-lzo.jar in path
			   conf.setInputFormat( DeprecatedLzoTextInputFormat.class );
			   
		   } else {
			   
			   System.out.println( "Using TextInputFormat" );

			   // for basic non-compressed formats:
			   conf.setInputFormat( TextInputFormat.class );
			   
		   }
		   
		   conf.setOutputFormat(TextOutputFormat.class);
		   conf.setCompressMapOutput(true);	   
		   
		   if (bCompressOutput) {
			   
			   System.out.println( "Using LZO To Compress the job Output" );
			   
			   TextOutputFormat.setOutputCompressorClass(conf, com.hadoop.compression.lzo.LzopCodec.class);
			   TextOutputFormat.setCompressOutput(conf, true);

		   } else {
			   
			   System.out.println( "No compression on job Output" );
			   
		   }
		   
		  		   
		   FileInputFormat.setInputPaths( conf, other_args.get(0) );
		   FileOutputFormat.setOutputPath( conf, new Path(other_args.get(1)) );
		   
		   
		   
		   JobClient.runJob(conf);
		   
		   return 0;
	   }
	   
		 
		
		 static int printUsage() {
		   System.out.println("ReadLZOFileExampleJob [-m <maps>] [-r <reduces>] <input> <output>");
		   ToolRunner.printGenericCommandUsage(System.out);
		   return -1;
		 }

	   
		 public static void main(String[] args) throws Exception {
		   
			int res = ToolRunner.run( new Configuration(), new ReadLZOFileExampleJob(), args );
		    System.exit(res);
		 
		 }
	   
	   
	}

