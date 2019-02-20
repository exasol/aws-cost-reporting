#!/usr/bin/perl -w 

# StdIn: 
# arn:aws:ec2:eu-central-1:602894545240:volume/vol-0c3d020bc910255a4
# Target command:
# aws --region eu-west-1 ec2 delete-volumes --volume-ids vol-0a263923cb3daed2b

use strict;
use warnings;

# Read StdIn
while (<>) {
    print STDERR "DEBUG: Deleting $_\n";
    if (m/arn:aws:ec2:([^:]+):602894545240:volume\/(vol-.+)$/) {
        system "aws --region $1 ec2 describe-volumes --volume-ids $2"
    }
    else {
        print STDERR "ERROR: Cannot parse $_\n";
    }
}
