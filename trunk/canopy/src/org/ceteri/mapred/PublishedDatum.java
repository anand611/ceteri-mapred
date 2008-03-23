/**
 * Copyright 2008 Paco NATHAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ceteri.mapred;

import java.io.BufferedReader;
import java.io.FileReader;

import java.util.LinkedList;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;


/**
 * Represents one data point in the sample being clustered.
 *
 * @author Paco NATHAN
 * @see <a href="http://code.google.com/p/ceteri-mapred/">Google Code project</a>
 */

public class
    PublishedDatum
    extends Datum
{
    /**
     * Protected members.
     */

    protected long year = 0L;
    protected long size = 0L;
    protected double year_value = 0.0D;
    protected double size_value = 0.0D;

    protected final static SummaryStatistics year_stats = new SummaryStatistics();
    protected final static SummaryStatistics size_stats = new SummaryStatistics();


    /**
     * Constructor.
     *
     * @param line	a string inputs one line from the data file
     */

    public 
	PublishedDatum (final String line)
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
     *
     * @param d	the other data point to compare with for calculating a distance
     * @return	the distance metric between these two points
     */

    public double
	getDistance (final Datum d)
    {
	final PublishedDatum that = (PublishedDatum) d;

	final double diff_year = this.year_value - that.year_value;
	final double diff_size = this.size_value - that.size_value;

	return Math.sqrt(((diff_year * diff_year) +
			  (diff_size * diff_size)
			  ) / 2.0D
			 );
    }


    /**
     * Render as a String.
     *
     * @return	a string representing the key/value of this data point
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
     * Load the data from a CSV text file.
     *
     * @param data_file	the file name for the input data to read
     * @return	a linked list of input data points
     * @throws Exception	if an exception occurred
     */

    public static LinkedList<PublishedDatum>
	loadData (final String data_file)
	throws Exception
    {
	final LinkedList<PublishedDatum> data_list =
	    new LinkedList<PublishedDatum>();

        final BufferedReader buf_reader =
	    new BufferedReader(new FileReader(data_file));

        String line = buf_reader.readLine();

        while (line != null) {
	    final PublishedDatum d = new PublishedDatum(line);
	    data_list.add(d);

            line = buf_reader.readLine();
        }

	return data_list;
    }


    /**
     * Normalize the values within the range [0.0, 1.0] for both year
     * and size.
     *
     * @param year_min	the minimum value for year in the population
     * @param year_range	the range of values for year in the population
     * @param size_min	the minimum value for size in the population
     * @param size_range	the range of values for size in the population
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
     *
     * @return	a double representing the "midpoint" of the data population
     */

    public static double
	getMeanDistance (final LinkedList<PublishedDatum> data_list)
    {
	final double year_min = year_stats.getMin();
	final double year_range = year_stats.getMax() - year_min;
	final double size_min = size_stats.getMin();
	final double size_range = size_stats.getMax() - size_min;

	for (Object obj : data_list) {
	    final PublishedDatum d = (PublishedDatum) obj;
	    d.normalize(year_min, year_range, size_min, size_range);
	}

	final PublishedDatum midpoint =
	    new PublishedDatum("," +
			       Math.round(year_stats.getMean()) +
			       "," +
			       Math.round(size_stats.getMean())
			       );

	midpoint.normalize(year_min, year_range, size_min, size_range);

	final PublishedDatum d1 =
	    (PublishedDatum) data_list.getFirst();

	return midpoint.getDistance(d1);
    }
}
