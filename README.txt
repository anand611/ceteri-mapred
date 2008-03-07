README for http://code.google.com/p/ceteri-mapred/

Code examples based on the Hadoop open source project to explore
applications of MapReduce.


	Example 1: jyte.com "cred" graph

In a graph of "cred" data from jyte.com the end-users -- identified by
their respective OpenID URIs -- give each other specifc tags for
credibility. The graph can be analyzed using a simplified PageRank
(i.e., one which has no damping term) to produce rank values that
correlate with the proprietary "cred points" determined by JanRain.

Directions:
   1. install version 0.15.3 of Hadoop
   2. work in the "jyte" directory
   3. download the "cred" data from http://jyte.com/site/api
   4. gunzip the "cred" data TWICE, saved as "cred.txt"
   5. edit properties as need in the first part of "build.xml"
   6. run "ant" to execute the build script
   7. results get stored as TSV files in "prevrank"

References:
   http://jyte.com
   http://groups.google.com/group/jyte
   http://en.wikipedia.org/wiki/PageRank

Contact:
   Paco NATHAN <pacoid@cs.stanford.edu>
