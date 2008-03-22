package org.ceteri.mapred;

import java.io.BufferedReader;
import java.io.FileReader;

import java.util.LinkedList;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;


/**
 * Represents one data point in the sample being clustered.
 *
 * @author Paco NATHAN http://code.google.com/p/ceteri-mapred/
 */

public class
    Datum
{
    public final static SummaryStatistics year_stats = new SummaryStatistics();
    public final static SummaryStatistics size_stats = new SummaryStatistics();


    /**
     * Public members.
     */

    public String key = null;
    public boolean marked = false;

    /**
     * Protected members.
     */

    protected long year = 0L;
    protected long size = 0L;
    protected double year_value = 0.0D;
    protected double size_value = 0.0D;


    /**
     * Constructor.
     */

    public 
	Datum (final String line)
    {
	final String[] token = line.split(",");

	this.key = token[0];
	this.year = Long.parseLong(token[1]);
	this.size = Long.parseLong(token[2]);

	year_stats.addValue(year);
	size_stats.addValue(size);
    }


    /**
     * Calculate the distance from another point.
     */

    public double
	getDistance (final Datum that)
    {
	final double diff_year = this.year_value - that.year_value;
	final double diff_size = this.size_value - that.size_value;

	return Math.sqrt(((diff_year * diff_year) +
			  (diff_size * diff_size)
			  ) / 2.0D
			 );
    }


    /**
     * Render as a String.
     */

    public String
	toString ()
    {
	final StringBuilder sb = new StringBuilder();

	sb.append('(');
	sb.append(key);
	sb.append(' ');
	sb.append(year);
	sb.append(' ');
	sb.append(size);
	sb.append(')');

	return sb.toString();
    }


    /**
     * Load the data from a text file
     */

    public static void
	loadData (final String data_file, final LinkedList<Datum> data_list)
	throws Exception
    {
        final BufferedReader buf_reader =
	    new BufferedReader(new FileReader(data_file));

        String line = buf_reader.readLine();

        while (line != null) {
	    final Datum d = new Datum(line);
	    data_list.add(d);

            line = buf_reader.readLine();
        }
    }


    /**
     * Normalize the values, within [0.0, 1.0]
     */

    public void
	normalize (final double year_min, final double year_range, final double size_min, final double size_range)
    {
	year_value = ((double) year - year_min) / year_range;
	size_value = ((double) size - size_min) / size_range;
    }


    /**
     * Calculate a "midpoint" within the data population, to use for
     * setting thresholds.
     */

    public static double
	getMeanDistance (final LinkedList<Datum> data_list)
    {
	final double year_min = year_stats.getMin();
	final double year_range = year_stats.getMax() - year_min;
	final double size_min = size_stats.getMin();
	final double size_range = size_stats.getMax() - size_min;

	for (Object obj : data_list) {
	    final Datum d = (Datum) obj;
	    d.normalize(year_min, year_range, size_min, size_range);
	}

	final Datum midpoint =
	    new Datum("," + Math.round(year_stats.getMean()) + "," + Math.round(size_stats.getMean()));

	midpoint.normalize(year_min, year_range, size_min, size_range);

	final Datum d1 = (Datum) data_list.getFirst();

	return midpoint.getDistance(d1);
    }
}
