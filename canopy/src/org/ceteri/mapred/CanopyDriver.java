package org.ceteri.mapred;

import java.io.BufferedReader;
import java.io.FileReader;

import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.math.util.MathUtils;


/**
 * Java implementation of the Canopy Clustering algorithm described in
 * McCallum, Nigam, Ungar:
 * http://www.kamalnigam.com/papers/canopy-kdd00.pdf
 *
 * @author Paco NATHAN http://code.google.com/p/ceteri-mapred/
 */

public class
    CanopyDriver
{
    /**
     * Public definitions.
     */

    public final static double LOOSE_TIGHT_RATIO = 0.7D;


    /**
     * Protected members.
     */

    protected final LinkedList<Datum> data_list = new LinkedList<Datum>();
    protected final HashSet<Canopy<Datum>> canopy_set = new HashSet<Canopy<Datum>>();

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
	    final Datum d = new Datum(line);
	    data_list.add(d);

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
	final double mean_distance = Datum.getMeanDistance(data_list);

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
	Datum picked = null;
	Canopy<Datum> canopy = null;

	while (unmarked > 0) {
	    picked = null;
	    canopy = new Canopy<Datum>();
	    canopy_set.add(canopy);

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
	final CanopyDriver c = new CanopyDriver();

	long start_time = 0L;
	long elapsed_time = 0L;

	// load the data from a text file

	c.loadData(data_file);

	// PASS 1: normalize data, set thresholds

	start_time = System.currentTimeMillis();
	c.normalizeData();
	elapsed_time = System.currentTimeMillis() - start_time;

	System.out.println("ELAPSED: " + elapsed_time);
	System.out.println("tight threshold: " + MathUtils.round(c.tight_threshold, 2));
	System.out.println("loose threshold: " + MathUtils.round(c.loose_threshold, 2));

	// PASS 2: create canopies

	start_time = System.currentTimeMillis();
	c.createCanopies();
	elapsed_time = System.currentTimeMillis() - start_time;

	// report results

	System.out.println("ELAPSED: " + elapsed_time);
	System.out.println("canopy count: " + c.canopy_set.size());

	for (Canopy<Datum> canopy : c.canopy_set) {
	    System.out.println(canopy);
	}
    }
}
