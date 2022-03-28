#!/usr/bin/perl

# Disclaimer:
# Script to support AKAMAI End Of Life Stream Packaging On Demand (SPOD) and migrate the content by leveraging NetStorage as a Downloader for segments.
# This script running from a laptop or server will monitor and control the download process.
# After End Of May 2022, Stream Packaging On Demand will be removed from Akamai.
# So in case you need to migrate content to NetStorage and continue to serve that content, this script is for you.
# After End Of May, this script will be useless.

# Author: FREDERIC BELETEAU / Enterprise Media Architect
# fbeletea@akamai.com / frederic.beleteau@gmail.com

#use strict;
#use warnings;
use Getopt::Long 'HelpMessage';
use File::stat;
use LWP::UserAgent;

# url of the master playlist
my $playlist = "https://yourAkamaiSPODhostname-vh.akamaihd.net/i/path/filename_,bitrate1,bitrate2,...,.mp4.csmil/master.m3u8";
# path to your SSH NetStorage key you deployed
my $nsKey = "/Users/myUser/.ssh/netstorage/myPublicNetStorageKey.pub";
# Hostname of the NetStorage group you will use as destination NetStorage
my $netstorage_host = "myAccount.upload.akamai.com";
# that is the path on destination NS from root, it could be cpcode/path or path based on NS user settings
my $cp_code = "myRootNSdirectory";
#Policy NameLivestream-f Token Auth, set the token key below and a token will be append to the stream url after the question mark as query parameters.
my $tokenKey = '';
my $TOKEN=""; # variable supporting the token creation

my $starttime = time;

use Term::ANSIColor qw(:constants);
use constant WARNINGCOLOR => MAGENTA; #magenta
use constant ERRORCOLOR => RED;
use constant DEBUGCOLOR => GREEN;
use constant INFOCOLOR => YELLOW; #blue

my $resume = 0;
my $resumeEND = 0;
my $log = 1;
my $debug = 0;
my $filetoload = "";
my $columnToExtract = -1;
my $start = 0;
my $end = 0;
my $rev = 0;
my $help = 0;
my $preview = 0;
my $downloader = "NS-hlsdownloader-VOD.pl";

my $nbArgs = $#ARGV + 1;
print INFOCOLOR,"\n";
print "--------------------------------------------------------------------------------------------\n";
print "               			 AKAMAI NETSTORAGE HLS DOWNLOADER VOD\n";
print "                           A MIGRATION TOOL for SPOD -> AMD+NS\n";
print "         (Stream Packaging to NetStorage, then serve content using Adaptive Media Delivery)\n";
print "--------------------------------------------------------------------------------------------\n\n",RESET;

GetOptions(\%args,
    '-file=s'    => \$filetoload,
	'-start=i'    => \$start,
    '-end=i'     => \$end,
    '-reverse!' => \$rev,
    '-nsHost=s'     => \$netstorage_host,
    '-nsPath=s'     => \$cp_code,
    '-nsKey=s'     => \$nsKey,
    '-tokenKey=s'  => \$tokenKey,
    '-preview!' => \$preview,
    '-log!' => \$log,
    '-debug!' => \$debug,
    '-h!'     => sub { HelpMessage(0) },
) or HelpMessage(1);

# tbc
sub print_help { ... }

=pod

Migration tool for Akamai Stream Packaging to NetStorage (->Static Packaging), migrate a bunch of stream urls from an input text file.

=head1 USAGE

perl processListOfStreams.pl -file=listOfStreamUrls.txt -nsHost=NetStorage_Hostname -nsPath=NetStorage_Destination_Directory -nsKey=NetStorage_SSH_Public_Key [-tokenKey=key] [-log] [-debug] [-preview]

  --file,-f 	 file to load (stream urls list)
  --start,-s     Start index for resume (optional)
  --end,-e       End index (optional)
  --reverse,-r   Reverse the list file (optional)
  --nsHost,-h    Netstorage hostname
  --nsPath,-p    Netstorage path
  --nsKey,-k'    Netstorage Key path
  --tokenKey,-t  Token key (optional)
  --preview,-p   Run in preview mode without running the download sub-process
  --debug,-d     Run in debug mode
  --help,-h      Print this help

=head2 EXAMPLE

    perl processListOfStreams.pl \
	--file="myLocalTextFileWithStreamUrls.txt" \
	--nsHost="myAccount.upload.akamai.com" \
	--nsPath="myRootNSdirectory" \
	--nsKey="/Users/myUser/.ssh/netstorage/myPublicNetStorageKey.pub" \
	[--tokenKey=myTokenKey] \
	[--preview] \
	[--log] \
	[--debug]

=head3 VERSION

0.1

=cut


# we have a start parameter
if ( $start>0 ){
	$resume = $start;
}
# we have an end parameter
if ( $end>0 ){
	$resumeEND = $end;
}

if ( $debug>0 ){
	$resumeEND = $end;
}

print "file = $filetoload\n";
print "start = $resume\n";
print "end = $resumeEND\n";
print "reverse = $rev\n";
print "nsHost: $netstorage_host\n";
print "nsPath: $cp_code\n";
print "nsKey: $nsKey\n";
print "tokenKey: $tokenKey\n";
print "log: $log\n";
print "debug: $debug\n";
print "preview: $preview\n";
print "-------------------------------------------------------------------------------------\n";

($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
$mon++;
$day = sprintf "%02d", $mday;
$month = sprintf "%02d", $mon;
$year = $year+1900;
$date = "$year$month$day";


sub loadFile {
	my $filetoload = shift;
	print "Loading file: $filetoload\n";
	print CSVFILE "Loading file,$filetoload,OK\n";
	open my $handle, '<', $filetoload;
	chomp(my @lines = <$handle>);
	close $handle;
	return @lines;
}

# inject raw data below in case there's no output file to load
my $testData = <<'END_DATA';
https://yourAkamaiSPODhostname-vh.akamaihd.net/i/path/filename1_,bitrate1,bitrate2,...,.mp4.csmil/master.m3u8
https://yourAkamaiSPODhostname-vh.akamaihd.net/i/path/filename2_,bitrate1,bitrate2,...,.mp4.csmil/master.m3u8
https://yourAkamaiSPODhostname-vh.akamaihd.net/i/path/filename3_,bitrate1,bitrate2,...,.mp4.csmil/master.m3u8

END_DATA

#------------------------------------------------------------------------------------------------------------------------------------------------------

my $newLine = "";
my $filename = "";
my $sizeOf = 0;
my $starttime = time;

#switch between both lines below to select the dataset from data abobe or data from file feed
my @cmd_out;
if ($filetoload eq "") {
    @cmd_out = split "\n", $testData;	# 1- use the demo dataset instead, no file was provided
} else {
    @cmd_out = loadFile($filetoload);	# 2- use an external list of stream urls extracted from backend
}

# reverse the list if expected from command line
if ( $rev > 0){
    print "-reverse the list-\n";
    my @cmd_out_reverse = reverse @cmd_out;
    @cmd_out = @cmd_out_reverse;
}
if ( $debug>0 ){
	print @cmd_out;
}
print "\n";
my $duration = time - $starttime;
my $durationMin = $duration/60;

print "$localTab\t- File list exploration took $duration s = $durationMin min\n\n";

my $lenOUT = scalar @cmd_out;
my $lineID = 0;
print "-nb lines: $lenOUT\n";

# if end is not set, set it to length of the array
if ( $end == 0 ) {
    $resumeEND = $lenOUT;
}

my @streamtypes;
my @streamids;
print "$baseurl\n";
print "\n---\n";

my @nsgroupids;
my $nbid = 0;

#------------------------------------------------------------------------------------------------------------------------------------------------------
# loop in the data array, on each stream url lines
	foreach $newLine (@cmd_out) {

		$lineID = $lineID+1;		
		if ( $resume > 0 ) {
			if ( $lineID < $resume ) {
				next;
			}
			if ( $lineID > $resumeEND ) {
				last;
			}
		} elsif ( $resumeEND > 0) {
			if ( $lineID > $resumeEND ) {
				last;
			}
		}

		print "---------------------------------------------------------------------------------------------------------------------------------\n";
		print "$localTab- $lineID/$lenOUT : ";

		# check if we have a valid line with the expected pattern
		if ($newLine =~ /^((https?:\/\/.*\.akamaihd\.net\/i\/)(.*)\/([\w\+\-]+)_,.*,.mp4.csmil\/)?([\w\+\-]+)\.m3u8(\??.*)$/s) {
			$playlist = $newLine;

			my $commandDownload = "";
			$commandDownload .= "-url=\"$playlist\" ";
			$commandDownload .= "-nsHost=\"$netstorage_host\" ";
			$commandDownload .= "-nsPath=\"$cp_code\" ";
			$commandDownload .= "-nsKey=\"$nsKey\" ";
			if (!($tokenKey eq "")) {
				$commandDownload .= "-t=\"$tokenKey\" ";
			}
			if ($debug>0) {
				$commandDownload .= "-debug ";
			}
			
			if ($preview eq 0) {
				print INFOCOLOR, "$playlist\n", RESET;
				if ($debug) {
					print INFOCOLOR, "perl $downloader $commandDownload 2>&1\n", RESET;
				}
				# processing the download, the command run below doesn't provide runtime output during the process
				#my @downloadProcess = `perl ./NS-hlsdownloader-VOD.pl $commandDownload 2>&1`;

				# so a better option is to stream the output and print
				open(INPUT, "perl $downloader $commandDownload |") or die $!;
				while(<INPUT>) {
					# Monitor process here, if required we could react to some events during the process
					#if(/regex/) { dosomething($_); }
					print;
				}
				close INPUT;
			} else {
				# useful for a dry run on the list to check as process can take some time
				print INFOCOLOR, "Preview: $playlist\n", RESET;
				if ($debug>0) {
					print INFOCOLOR, "Preview: perl $downloader $commandDownload 2>&1\n", RESET;
				}
			}
		} else {
			$playlist = "";
			print "\n";
		}

	}

	print "---------------------------------------------------------------------------------------------------------------------------------\n";


print "\n\n";
close(CSVFILE);
print "Done!\n";

my $duration = time - $starttime;
my $durationMin = $duration/60;
	
print "$localTab\t- Process took $duration s = $durationMin min\n\n";
