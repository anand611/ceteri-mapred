#!/usr/bin/perl -w

# crawl jyte.com profiles to extract cred point values for specific
# OpenID URIs
#
# Paco NATHAN http://code.google.com/p/ceteri-mapred/

$, = "\t";    # set output field separator
$\ = "\n";    # set output record separator

require 5.005;
use strict;

my $debug = 1; # 0;


MAIN:
{
    while (<STDIN>) {
	chop;
	print if ($debug > 1);

	my ($uri, $rank) = split(/\t/);
	my $err = `wget -o /dev/null -O crawl.html http://jyte.com/profile/$uri`;

	print $uri, $rank, &parseCred;
    }
}


sub
    parseCred
{
    my $last_line;

    open(WGET, "crawl.html");

    while (<WGET>) {
	chop;

	if (/\S/) {
	    if (/\<h3\>Cred Points\<\/h3\>/) {
		close(WGET);

		$last_line =~ s/^\s+\<h1.*\>([\d\.]+)\<.*$/$1/g;
		return $last_line;
	    }

	    $last_line = $_;
	}
    }
}
