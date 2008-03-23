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


/**
 * Represents one canopy cluster for data points of generic type T.
 *
 * @author Paco NATHAN
 * @see <a href="http://code.google.com/p/ceteri-mapred/">Google Code project</a>
 */

public class
    Canopy<T extends Datum>
    extends HashSet<T>
{
    /**
     * Protected members.
     */

    T prototype = null;


    /**
     * Set the cluster prototype.
     *
     * @param t	a data point used as the prototype for this cluster
     */

    public void
	setPrototype (final T t)
    {
	prototype = t;
	add(t);
    }


    /**
     * Get the cluster prototype.
     *
     * @return	a data point used as the prototype for this cluster
     */

    public T
	getPrototype ()
    {
	return prototype;
    }
}
