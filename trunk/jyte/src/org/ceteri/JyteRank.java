package org.ceteri;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Prepares data which has been downloaded from the jyte.com API cred
 * database: http://jyte.com/static/misc/cred.txt.gz
 * @author Paco NATHAN http://www.spock.com/Paco-Nathan
 */

public class
    JyteRank
    extends Configured
    implements Tool
{
    public static final int ITERATIONS = 9;

    /**
     * User counters.
     */

    protected static enum MyCounters { CRED_GIVER }
    protected static final Counters counters = new Counters();


    /**
     * User data definitions.
     */

    public static class
	TextArrayWritable
	extends ArrayWritable
    {
	public
	    TextArrayWritable ()
        {
	    super(Text.class);
        }


	public String
	    toString ()
        {
	    final StringBuilder sb = new StringBuilder();
	    final Writable[] uris = get();

	    for (int i = 0; i < uris.length; i++) {
		if (i > 0) {
		    sb.append('\t');
		}

		sb.append((Text) uris[i]);
	    }

	    return sb.toString();
	}
    }


    //////////////////////////////////////////////////////////////////////
    // PASS 1: prepare data from the jyte API download
    //////////////////////////////////////////////////////////////////////

    /**
     * PASS 1.
     * Prepares the data from the jyte API download.
     * line format: from_openid      to_openid       csv cred tags
     */

    public static class
	MapFrom2To
	extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, TextArrayWritable>
    {
	private final Text from_openid = new Text();
	private final Text to_openid = new Text();
	private final Text[] uris = new Text[1];
	private final TextArrayWritable to_list = new TextArrayWritable();

	public void
	    map (final LongWritable key, final Text value, final OutputCollector<Text, TextArrayWritable> output, final Reporter reporter)
	    throws IOException
        {
	    final String line_text = value.toString();

	    if (!line_text.startsWith("#")) {
		final String[] line = line_text.split("\\t");

		from_openid.set(line[0]);
		to_openid.set(line[1]);

		uris[0] = to_openid;
		to_list.set(uris);

		output.collect(from_openid, to_list);
	    }
	}
    }
  

    /**
     * PASS 1.
     * Reducers emits a one-to-many mapping between from_openid and
     * its to_openid list items.
     */

    public static class
	RedFrom2To
	extends MapReduceBase
	implements Reducer<Text, TextArrayWritable, Text, TextArrayWritable>
    {
	public void
	    reduce (final Text key, final Iterator<TextArrayWritable> values, final OutputCollector<Text, TextArrayWritable> output, final Reporter reporter)
	    throws IOException
        {
	    final ArrayList<Text> to_uri = new ArrayList<Text>();

	    while (values.hasNext()) {
		final Writable[] to_openid = values.next().get();

		for (int i = 0; i < to_openid.length; i++) {
		    to_uri.add((Text) to_openid[i]);
		}
	    }

	    final TextArrayWritable to_list = new TextArrayWritable();
	    final Text[] uris = new Text[to_uri.size()];

	    for (int i = 0; i < to_uri.size(); i++) {
		uris[i] = to_uri.get(i);
	    }

	    to_list.set(uris);
	    output.collect(key, to_list);
	}
    }

  
    //////////////////////////////////////////////////////////////////////
    // PASS 2: prepare initial values in the rank vector
    //////////////////////////////////////////////////////////////////////

    /**
     * PASS 2.
     * Prepares initial values in the rank vector
     * line format: from_openid      tsv to_openid
     */

    public static class
	MapInitRank
	extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, FloatWritable>
    {
	private final static FloatWritable ZERO = new FloatWritable(0.0f);
	private final Text from_openid = new Text();

	public void
	    map (final LongWritable key, final Text value, final OutputCollector<Text, FloatWritable> output, final Reporter reporter) 
	    throws IOException
        {
	    final String[] line = value.toString().split("\\t");

	    from_openid.set(line[0]);
	    output.collect(from_openid, ZERO);
	}
    }
  

    /**
     * PASS 2.
     * Emits a "1.0" as the initial rank value for each OpenID URI
     * noted in the cred system.
     */

    public static class
	RedInitRank
	extends MapReduceBase
	implements Reducer<Text, FloatWritable, Text, FloatWritable>
    {
	private final static FloatWritable ONE = new FloatWritable(1.0f);

	public void
	    reduce (final Text key, final Iterator<FloatWritable> values, final OutputCollector<Text, FloatWritable> output, final Reporter reporter)
	    throws IOException
        {
	    counters.incrCounter(MyCounters.CRED_GIVER, 1);
	    output.collect(key, ONE);
	}
    }


    //////////////////////////////////////////////////////////////////////
    // PASS 3: expand terms in a numerical solution for the diff eq
    //////////////////////////////////////////////////////////////////////

    /**
     * PASS 3.
     * Perform a join.
     */

    public static class
	MapExpand
	extends MapReduceBase
	implements Mapper<LongWritable, Writable, Text, Text>
    {
	private final Text from_openid = new Text();

	public void
	    map (final LongWritable key, final Writable value, final OutputCollector<Text, Text> output, final Reporter reporter)
	    throws IOException
        {
	    final String[] line = value.toString().split("\\t");
	    from_openid.set(line[0]);

	    try {
		// continue only when value is a float

		final float rank = Float.parseFloat(line[1]);
		output.collect(from_openid, new Text("r " + line[1]));
	    }
	    catch (Exception e) {
		// otherwise, parse the list of to_openid

		final TextArrayWritable to_list = new TextArrayWritable();
		final Text[] uris = new Text[line.length - 1];

		for (int i = 1; i < line.length; i++) {
		    uris[i - 1] = new Text(line[i]);
		}

		to_list.set(uris);
		output.collect(from_openid, new Text("u " + to_list.toString()));
	    }
	}
    }
  

    /**
     * PASS 3.
     * Emit the joined record.
     */

    public static class
	RedExpand
	extends MapReduceBase
	implements Reducer<Text, Text, Text, FloatWritable>
    {
	public void
	    reduce (final Text key, final Iterator<Text> values, final OutputCollector<Text, FloatWritable> output, final Reporter reporter)
	    throws IOException
        {
	    final ArrayList<String> to_openid = new ArrayList<String>();
	    float rank = 0.0f;

	    while (values.hasNext()) {
		final String line = values.next().toString();

		if (line.startsWith("r ")) {
		    try {
			rank = Float.parseFloat(line.substring(2));
		    }
		    catch (Exception e) {
			System.out.println("float error: " + line);
		    }
		} else {
		    final String[] uris = line.substring(2).split("\\t");

		    for (int i = 0; i < uris.length; i++) {
			to_openid.add(uris[i]);
		    }
		}
	    }

	    for (int i = 0 ; i < to_openid.size(); i++) {
		final float jr = rank / (float) to_openid.size();

		output.collect(new Text(to_openid.get(i)), new FloatWritable(jr));
	    }
	}
    }


    //////////////////////////////////////////////////////////////////////
    // PASS 4: sum terms in a numerical solution for the diff eq
    //////////////////////////////////////////////////////////////////////

    /**
     * PASS 4.
     * Collects terms to be summed.
     */

    public static class
	MapSum
	extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, FloatWritable>
    {
	private final Text to_openid = new Text();
	private final FloatWritable rank = new FloatWritable();

	public void
	    map (final LongWritable key, final Text value, final OutputCollector<Text, FloatWritable> output, final Reporter reporter)
	    throws IOException
        {
	    try {
		final String[] line = value.toString().split("\\t");

		to_openid.set(line[0]);
		rank.set(Float.parseFloat(line[1]));
		output.collect(to_openid, rank);
	    }
	    catch (Exception e) {
		System.err.println("float error: " + value.toString());
	    }
	}
    }
  

    /**
     * PASS 4.
     * Calculate the sum of terms to determine a final "page rank" for
     * each OpenID in the cred graph.
     */

    public static class
	RedSum
	extends MapReduceBase
	implements Reducer<Text, FloatWritable, Text, FloatWritable>
    {
	private final FloatWritable rank = new FloatWritable();

	public void
	    reduce (final Text key, final Iterator<FloatWritable> values, final OutputCollector<Text, FloatWritable> output, final Reporter reporter)
	    throws IOException
        {
	    float sum = 0.0f;

	    while (values.hasNext()) {
		sum += values.next().get();
	    }

	    rank.set(sum);
	    output.collect(key, rank);
	}
    }

  
    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
  
    protected final List<String> other_args = new ArrayList<String>();

    protected Path input_path = null;
    protected Path from2to_path = null;
    protected Path prevrank_path = null;
    protected Path elemrank_path = null;
    protected Path thisrank_path = null;


    /**
     * Main entry point.
     */

    public static void
	main (final String[] args)
	throws Exception
    {
	final int result =
	    ToolRunner.run(new Configuration(), new JyteRank(), args);

	System.exit(result);
    }


    /**
     * Print the command-line usage text.
     */

    protected static int
	printUsage ()
    {
	System.out.println("jyterank <input> <from2to> <prevrank> <elemrank> <thisrank>");
	ToolRunner.printGenericCommandUsage(System.out);

	return -1;
    }

  
    /**
     * Invoke this method to submit the map/reduce job.
     * @throws IOException When there is communication problems with the job tracker.
     */

    public int
	run (final String[] args)
	throws Exception
    {
	boolean only_iterate = false;

	for (int i = 0; i < args.length; ++i) {
	    try {
		if ("-i".equals(args[i])) {
		    only_iterate = true;
		} else {
		    other_args.add(args[i]);
		}
	    } catch (NumberFormatException except) {
		System.out.println("ERROR: Integer expected instead of " + args[i]);
		return printUsage();
	    } catch (ArrayIndexOutOfBoundsException except) {
		System.out.println("ERROR: Required parameter missing from " + args[i-1]);
		return printUsage();
	    }
	}

	input_path = new Path(other_args.get(0));
	from2to_path = new Path(other_args.get(1));
	prevrank_path = new Path(other_args.get(2));
	elemrank_path = new Path(other_args.get(3));
	thisrank_path = new Path(other_args.get(4));

	// PASS 1: prepare data from jyte API download
	// PASS 2: prepare initial values in the rank vector

	if (!only_iterate) {
	    JobClient.runJob(configPass1());
	    JobClient.runJob(configPass2());
	}

	// PASS 3: expand terms in a numerical solution for the diff eq
	// PASS 4: sum terms in a numerical solution for the diff eq

	for (int i = 0; i < ITERATIONS; i++) {
	    JobClient.runJob(configPass3());
	    JobClient.runJob(configPass4());

	    final FileSystem fs = FileSystem.get(getConf());

	    fs.delete(prevrank_path);
	    fs.delete(elemrank_path);
	    fs.rename(thisrank_path, prevrank_path);

	    prevrank_path = new Path(other_args.get(2));
	    elemrank_path = new Path(other_args.get(3));
	    thisrank_path = new Path(other_args.get(4));
	}

	// done

	System.out.println("N = " + counters.getCounter(MyCounters.CRED_GIVER));
	return 0;
    }


    /**
     * PASS 1: prepare data from jyte API download.
     */

    protected JobConf
	configPass1 ()
	throws Exception
    {
	final JobConf conf = new JobConf(getConf(), JyteRank.class);

	conf.setJobName("pass1");
    
	conf.setMapperClass(MapFrom2To.class);        
	conf.setCombinerClass(RedFrom2To.class);
	conf.setReducerClass(RedFrom2To.class);

	conf.setInputPath(input_path);
	conf.setOutputPath(from2to_path);
        
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(TextArrayWritable.class);

	return conf;
    }


    /**
     * PASS 2: prepare initial values in the rank vector.
     */

    protected JobConf
	configPass2 ()
	throws Exception
    {
	final JobConf conf = new JobConf(getConf(), JyteRank.class);

	conf.setJobName("pass2");
    
	conf.setMapperClass(MapInitRank.class);        
	conf.setReducerClass(RedInitRank.class);

	conf.setInputPath(from2to_path);
	conf.setOutputPath(prevrank_path);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	return conf;
    }


    /**
     * PASS 3: expand terms in a numerical solution for the diff eq.
     */

    protected JobConf
	configPass3 ()
	throws Exception
    {
	final JobConf conf = new JobConf(getConf(), JyteRank.class);

	conf.setJobName("pass3");
    
	conf.setMapperClass(MapExpand.class);        
	conf.setReducerClass(RedExpand.class);

	conf.setInputPath(from2to_path);
	conf.addInputPath(prevrank_path);
	conf.setOutputPath(elemrank_path);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	return conf;
    }


    /**
     * PASS 4: sum terms in a numerical solution for the diff eq.
     */

    protected JobConf
	configPass4 ()
	throws Exception
    {
	final JobConf conf = new JobConf(getConf(), JyteRank.class);

	conf.setJobName("pass4");
    
	conf.setMapperClass(MapSum.class);        
	conf.setReducerClass(RedSum.class);

	conf.setInputPath(elemrank_path);
	conf.setOutputPath(thisrank_path);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	return conf;
    }
}
