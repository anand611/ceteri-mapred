package org.ceteri.mapred;

import java.io.BufferedReader;
import java.io.FileReader;

import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.util.MathUtils;


/**
 * Java implementation of the Canopy Clustering algorithm described in
 * McCallum, Nigam, Ungar:
 * http://www.kamalnigam.com/papers/canopy-kdd00.pdf
 *
 * @author Paco NATHAN http://code.google.com/p/ceteri-mapred/
 */

public class
    Canopy
{
    /**
     * Public definitions.
     */

    public final static double LOOSE_TIGHT_RATIO = 0.8D;


    /**
     * Protected members.
     */

    protected final LinkedList<Datum> data_list = new LinkedList<Datum>();
    protected final HashSet<HashSet<Datum>> canopy_set = new HashSet<HashSet<Datum>>();

    protected final SummaryStatistics year_stats = new SummaryStatistics();
    protected final SummaryStatistics size_stats = new SummaryStatistics();

    protected double mean_distance = 0.0D;
    protected double loose_threshold = 0.0D;
    protected double tight_threshold = 0.0D;


    /**
     * Load the data from a text file
     */

    public void
	loadData (final String data_file)
	throws Exception
    {
        final BufferedReader buf_reader =
	    new BufferedReader(new FileReader(data_file));

        String line = buf_reader.readLine();

        while (line != null) {
	    final String[] token = line.split(",");

	    final Datum d = new Datum(token[0],
				      Long.parseLong(token[1]),
				      Long.parseLong(token[2])
				      );

	    data_list.add(d);
	    year_stats.addValue(d.year);
	    size_stats.addValue(d.size);

            line = buf_reader.readLine();
        }
    }


    /**
     * Normalize the data, set the thresholds.
     *
     * NB: override to set tight/loose thresholds manually; the
     * default data allows them to be determined automatically.
     */

    public void
	normalizeData ()
    {
	final double year_min = year_stats.getMin();
	final double year_range = year_stats.getMax() - year_min;
	final double size_min = size_stats.getMin();
	final double size_range = size_stats.getMax() - size_min;

	for (Datum d : data_list) {
	    d.normalize(year_min, year_range, size_min, size_range);
	}

	final Datum midpoint = new Datum("", 0L, 0L);

	midpoint.year = Math.round(year_stats.getMean());
	midpoint.size = Math.round(size_stats.getMean());
	midpoint.normalize(year_min, year_range, size_min, size_range);

	mean_distance = midpoint.getDistance(data_list.getFirst());
	loose_threshold = mean_distance;
	tight_threshold = mean_distance * LOOSE_TIGHT_RATIO;
    }


    /**
     * Create canopies. This method performs the heavy lifting.
     */

    public void
	createCanopies ()
    {
	int unmarked = data_list.size();

	while (unmarked > 0) {
	    final HashSet<Datum> canopy = new HashSet<Datum>();
	    canopy_set.add(canopy);

	    Datum picked = null;

	    for (Datum d : data_list) {
		if (!d.marked) {
		    if (picked == null) {
			picked = d;
			canopy.add(d);
			d.marked = true;
			unmarked--;

			//System.out.println("picked:\n" + d);
		    } else {
			final double distance = picked.getDistance(d);

			//System.out.println(d);
			//System.out.println(MathUtils.round(distance, 2));

			if (distance < loose_threshold) {
			    canopy.add(d);

			    if (distance < tight_threshold) {
				d.marked = true;
				unmarked--;
			    }
			}
		    }
		}
	    }
	}
    }


    /**
     * Main entry point.
     */

    public static void
	main (final String[] args)
	throws Exception
 {
	final String data_file = args[0];
	final Canopy c = new Canopy();

	long start_time = 0L;
	long elapsed_time = 0L;

	// load the data from a text file

	c.loadData(data_file);

	// PASS 1: normalize data, set thresholds

	start_time = System.currentTimeMillis();
	c.normalizeData();
	elapsed_time = System.currentTimeMillis() - start_time;

	System.out.println("ELAPSED: " + elapsed_time);
	System.out.println("mean distance: " + MathUtils.round(c.mean_distance, 2));

	// PASS 2: create canopies

	start_time = System.currentTimeMillis();
	c.createCanopies();
	elapsed_time = System.currentTimeMillis() - start_time;

	// report results

	System.out.println("ELAPSED: " + elapsed_time);
	System.out.println("canopy count: " + c.canopy_set.size());

	for (HashSet<Datum> canopy : c.canopy_set) {
	    System.out.println(canopy);
	}
    }
}
