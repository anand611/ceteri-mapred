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
     * Main entry point.
     */

    public static void
	main (final String[] args)
	throws Exception
 {
	final String data_file = args[0];

	long start_time = 0L;
	long elapsed_time = 0L;

	final LinkedList<Datum> data_list = new LinkedList<Datum>();
	final SummaryStatistics year_stats = new SummaryStatistics();
	final SummaryStatistics size_stats = new SummaryStatistics();

	// load the text from a file

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


	//////////////////////////////////////////////////
	// PASS 1: Normalize data, determine midpoint

	start_time = System.currentTimeMillis();

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

	final double mean_distance = midpoint.getDistance(data_list.getFirst());
	final double loose_threshold = mean_distance;
	final double tight_threshold = mean_distance * LOOSE_TIGHT_RATIO;

	elapsed_time = System.currentTimeMillis() - start_time;

	System.out.println("ELAPSED: " + elapsed_time);
	System.out.println("mean distance: " + MathUtils.round(mean_distance, 2));


	//////////////////////////////////////////////////
	// PASS 2: Create canopies

	final HashSet<HashSet<Datum>> canopy_set = new HashSet<HashSet<Datum>>();

	start_time = System.currentTimeMillis();
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

			System.out.println("picked:\n" + d);
		    } else {
			final double distance = picked.getDistance(d);

			System.out.println(d);
			System.out.println(MathUtils.round(distance, 2));

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

	elapsed_time = System.currentTimeMillis() - start_time;

	System.out.println("ELAPSED: " + elapsed_time);

	for (HashSet<Datum> canopy : canopy_set) {
	    System.out.println(canopy);
	}
    }
}
