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

import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.math.util.MathUtils;


/**
 * Java implementation of the Canopy Clustering algorithm described in
 * McCallum, Nigam, Ungar:
 * http://www.kamalnigam.com/papers/canopy-kdd00.pdf
 *
 * Represents a set of canopy clusters for data points of generic type T.
 *
 * @author Paco NATHAN
 * @see <a href="http://code.google.com/p/ceteri-mapred/">Google Code project</a>
 */

public class
    CanopyDriver<T extends Datum>
{
    /**
     * Public definitions.
     */

    public final static double LOOSE_TIGHT_RATIO = 0.85D;


    /**
     * Protected members.
     */

    protected HashSet<Canopy<T>> canopy_set = null;
    protected LinkedList<T> data_list = null;

    protected double loose_threshold = 0.0D;
    protected double tight_threshold = 0.0D;


    /**
     * Set the thresholds. NB: Override this to set tight/loose
     * thresholds manually; example test case allows them to be
     * determined automatically.
     *
     * @param mean_distance	the mean distance from the population "midpoint" to a random point
     */

    public void
	setThresholds (final double mean_distance)
    {
	loose_threshold = mean_distance;
	tight_threshold = mean_distance * LOOSE_TIGHT_RATIO;
    }


    /**
     * Create canopies. This method performs the heavy lifting. See
     * embedded notes from the research paper about algorithm steps.
     */

    public void
	createCanopies ()
    {
	canopy_set = new HashSet<Canopy<T>>();

	// McCallum, et al., 2.1.1: "start with a list of the data
	// points in any order, and with two distance thresholds..."

	T picked = null;
	Canopy<T> canopy = null;

	// McCallum, et al., 2.1.1: "repeat until the list is
	// empty..."

	int unmarked = data_list.size();

	while (unmarked > 0) {
	    // McCallum, et al., 2.1.1: "one can create canopies as
	    // follows..."

	    picked = null;
	    canopy = new Canopy<T>();
	    canopy_set.add(canopy);

	    for (T t : data_list) {
		if (!t.marked) {
		    if (picked == null) {
			// McCallum, et al., 2.1.1: "pick a point off
			// the list...

			picked = t;
			canopy.setPrototype(t);

			t.marked = true;
			unmarked--;

			//System.out.println("picked:\n" + t);
		    } else {
			// McCallum, et al., 2.1.1: " approximately
			// measure its distance to all other
			// points..."

			final double distance = picked.getDistance(t);

			//System.out.println(t);
			//System.out.println(MathUtils.round(distance, 2));

			// McCallum, et al., 2.1.1: "put all points
			// that are within distance threshold T1 into
			// a canopy..."

			if (distance < loose_threshold) {
			    canopy.add(t);

			    // McCallum, et al., 2.1.1: "remove from
			    // the list all points that are within
			    // distance threshold T2..."

			    if (distance < tight_threshold) {
				t.marked = true;
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
     *
     * @param args	the arguments from build script or command line
     * @throws Exception	if an exception occurred
     */

    public static void
	main (final String[] args)
	throws Exception
 {
	final String data_file = args[0];
	final CanopyDriver<PublishedDatum> c = new CanopyDriver<PublishedDatum>();

	long start_time = 0L;
	long elapsed_time = 0L;

	// PASS 1: load data, normalize data, set thresholds

	start_time = System.currentTimeMillis();

	c.data_list = PublishedDatum.loadData(data_file);
	c.setThresholds(PublishedDatum.getMeanDistance(c.data_list));

	elapsed_time = System.currentTimeMillis() - start_time;

	// report results

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

	for (Canopy<PublishedDatum> canopy : c.canopy_set) {
	    System.out.println("prototype: " + canopy.getPrototype());
	    System.out.println(canopy);
	}
    }
}
