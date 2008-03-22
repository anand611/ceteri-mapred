package org.ceteri.mapred;


/**
 * Represents one data point in the sample being clustered.
 *
 * @author Paco NATHAN http://code.google.com/p/ceteri-mapred/
 */

public class
    Datum
{
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
	Datum (final String label, final long year, final long size)
    {
	this.label = label;
	this.year = year;
	this.size = size;
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
}
