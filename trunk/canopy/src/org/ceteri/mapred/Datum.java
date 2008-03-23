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


/**
 * An abstract class representing one data point in the sample being
 * clustered.
 *
 * @author Paco NATHAN
 * @see <a href="http://code.google.com/p/ceteri-mapred/">Google Code project</a>
 */

public abstract class
    Datum
{
    /**
     * Public members.
     */

    public String key = null;
    public boolean marked = false;


    /**
     * Constructor.
     */

    public 
	Datum ()
    {
	// is lame. don't want.
	// compiler makes you do it.
	// not nice.
    }


    /**
     * Calculate the distance from another point.
     *
     * @param d	the other data point to compare with for calculating a distance
     * @return	the distance metric between these two points
     */

    public abstract double
	getDistance (final Datum that)
	;


    /**
     * Render as a String.
     *
     * @return	a string representing the key/value of this data point
     */

    public abstract String
	toString ()
	;
}
