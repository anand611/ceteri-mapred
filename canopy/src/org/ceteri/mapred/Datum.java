package org.ceteri.mapred;

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

    public String label = null;
    public long year = 0L;
    public long size = 0L;
    public double year_value = 0.0D;
    public double size_value = 0.0D;
    public boolean marked = false;


    /**
     * Constructor.
     */

    public 
	Datum (final String line)
    {
	final String[] token = line.split(",");

	this.label = token[0];
	this.year = Long.parseLong(token[1]);
	this.size = Long.parseLong(token[2]);

	year_stats.addValue(year);
	size_stats.addValue(size);
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
	sb.append(label);
	sb.append(' ');
	sb.append(year);
	sb.append(' ');
	sb.append(size);
	sb.append(')');

	return sb.toString();
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

	for (Datum d : data_list) {
	    d.normalize(year_min, year_range, size_min, size_range);
	}

	final Datum midpoint =
	    new Datum("," + Math.round(year_stats.getMean()) + "," + Math.round(size_stats.getMean()));

	midpoint.normalize(year_min, year_range, size_min, size_range);

	return midpoint.getDistance(data_list.getFirst());
    }
}
