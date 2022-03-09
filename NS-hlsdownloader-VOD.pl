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

#use Term::ANSIColor;
#print color("red"), "Stop!\n", color("reset");
#print color("green"), "Go!\n", color("reset");
#Or like this:

use Term::ANSIColor qw(:constants);
#print RED, "Stop!\n", RESET;
#print GREEN, "Go!\n", RESET;

# color definitions in the package
#      CLEAR           RESET             BOLD            DARK
#      FAINT           ITALIC            UNDERLINE       UNDERSCORE
#      BLINK           REVERSE           CONCEALED
#      BLACK           RED               GREEN           YELLOW
#      BLUE            MAGENTA           CYAN            WHITE
#      ON_BLACK        ON_RED            ON_GREEN        ON_YELLOW
#      ON_BLUE         ON_MAGENTA        ON_CYAN         ON_WHITE
#      BRIGHT_BLACK    BRIGHT_RED        BRIGHT_GREEN    BRIGHT_YELLOW
#      BRIGHT_BLUE     BRIGHT_MAGENTA    BRIGHT_CYAN     BRIGHT_WHITE
#      ON_BRIGHT_BLACK ON_BRIGHT_RED     ON_BRIGHT_GREEN ON_BRIGHT_YELLOW
#      ON_BRIGHT_BLUE  ON_BRIGHT_MAGENTA ON_BRIGHT_CYAN  ON_BRIGHT_WHITE

#our $WARNINGCOLOR = YELLOW;

use constant WARNINGCOLOR => MAGENTA; #magenta
use constant ERRORCOLOR => RED;
use constant DEBUGCOLOR => GREEN;
use constant INFOCOLOR => YELLOW; #blue

my $nbArgs = $#ARGV + 1;
print INFOCOLOR,"\n";
print "--------------------------------------------------------------------------------------------\n";
print "               			AKAMAI NETSTORAGE HLS DOWNLOADER VOD\n";
print "                           A MIGRATION TOOL for SPOD -> AMD+NS\n";
print "         (Stream Packaging to NetStorage, then serve content using Adaptive Media Delivery)\n";
print "--------------------------------------------------------------------------------------------\n\n",RESET;

GetOptions(\%args,
    '-url=s'    => \$playlist,
    '-nsHost=s'     => \$netstorage_host,
    '-nsPath=s'     => \$cp_code,
    '-nsKey=s'     => \$nsKey,
    '-tokenKey=s'  => \$tokenKey,
    '-log!' => \$log,
    '-debug!' => \$debug,
    '-h!'     => sub { HelpMessage(0) },
) or HelpMessage(1);

# tbc
sub print_help { ... }

=pod

Migration tool for Akamai Stream Packaging to NetStorage (->Static Packaging)

=head1 USAGE

perl NS-hlsdownloader-VOD.pl -url=streamUrl -nsHost=NetStorage_Hostname -nsPath=NetStorage_Destination_Directory -nsKey=NetStorage_SSH_Public_Key [-tokenKey=key] [-log] [-debug]

  --url,-u    	 Stream URL
  --nsHost,-h    Netstorage hostname
  --nsPath,-p    Netstorage path
  --nsKey,-k'    Netstorage Key path
  --tokenKey,-t  Token key (optional)
  --debug,-d     Run in debug mode
  --help,-h      Print this help

=head2 EXAMPLE

    perl NS-hlsdownloader-VOD.pl \
	-url="https://yourAkamaiSPODhostname-vh.akamaihd.net/i/path/filename_,bitrate1,bitrate2,...,.mp4.csmil/master.m3u8" \
	-nsHost="myAccount.upload.akamai.com" \
	-nsPath="myRootNSdirectory" \
	-nsKey="/Users/myUser/.ssh/netstorage/myPublicNetStorageKey.pub" \
	[-tokenKey=myTokenKey] \
	[-log] \
	[-debug]

=head3 VERSION

0.1

=cut


print "url: $playlist\n";
print "nsHost: $netstorage_host\n";
print "nsPath: $cp_code\n";
print "nsKey: $nsKey\n";
print "tokenKey: $tokenKey\n";
print "log: $log\n";
print "debug: $debug\n";
print "-------------------------------------------------------------------------------------\n";


# create a playlist directory where we modify the m3u8 from source directory
if (!-d "./playlist"){
	print INFOCOLOR,"Create playlist directory (temporary output directory used to rewrite playlists during the process)\n",RESET;
	system ("mkdir ./playlist");
}

# create a source directory where we download the source m3u8
if (!-d "./playlist_src"){
	print INFOCOLOR,"Create playlist_src directory (temporary input directory to download manifests)\n",RESET;
	system ("mkdir ./playlist_src");
}

if (!-e $nsKey){
	print ERRORCOLOR,"Error: Please check your netstorage key: $nsKey\n",RESET;
	exit(1);
}

($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
$mon++;
$day = sprintf "%02d", $mday;
$month = sprintf "%02d", $mon;
$year = $year+1900;
$date = "$year$month$day";

# compute Token within the script if required.
sub getToken
{
	my $TOKEN=`python ./akamai_token_v2.py -n hdnts -s now -w 20000000 -a '/*' -k $TOKENKEY`;
	$TOKEN =~ s/\r|\n//g;
	return $TOKEN;
}

sub loadFile {
	my $filetoload = shift;
	print "Loading file: $filetoload\n";
	print CSVFILE "Loading file,$filetoload,OK\n";
	open my $handle, '<', $filetoload;
	chomp(my @lines = <$handle>);
	close $handle;
	return @lines;
}

#function to preview 10 first lines of data
sub previewString {
	my (@string) = @_;
 	print "-------------------------------------------------------------------------------------\n";
    my @lines = split "\n", @string;
	print"Preview of 10 lines/@lines:\n";
	#my $first_ten = join "\n", @string[0 .. 9];
	my $first_ten = join "", @string[0 .. 9];
	print $first_ten;
	print "\n-------------------------------------------------------------------------------------\n";
}


#function to rewrite playlist
sub rewriteChildM3U8 {
	my($childm3u8, @string) = @_;
	print "Rewriting Child Playlist M3U8\n";
	open OUTFILECHILD, ">./playlist/$childm3u8\.m3u8" or die $!;
	my $tsPath="";
	my $ts="";
	my $tsQS="";
	foreach $line (@string) {
		$ts="";
		#if ($line=~/[\.ts|\.aac]/) {
			#print "rewrite $line\n";
			#if ($line=~/.*\/(^(.*\/)?([\w\+\-\.ts|\w\+\-\.aac]+)\??.*?)/i) { $ts = "$1";}
			if ($line=~/^(.*\/)?([\w\+\-\.ts|\w\+\-\.aac]+)(\??.*)$/i) {
				$tsPath = "$1";
				$ts = "$2";
				$tsQS = "$3";
			}
			my $lurl = "$line";
			chomp($lurl);
			if (!($ts eq "")) {
					#print "      | $childm3u8/$ts\n";
					print OUTFILECHILD "$childm3u8/$ts\n";
				} else {
					#print "      | $lurl\n";
					print OUTFILECHILD "$lurl\n";
				}
		#}
	}
	#print "\n-------------------------------------------------------------------------------------\n";
	close(OUTFILECHILD);
}


#function to rewrite playlist
sub rewriteMasterM3U8 {
	my($masterm3u8, $newpath, @string) = @_;
	print "Rewriting Master Playlist M3U8\n";
	open OUTFILEMASTER, ">./playlist/$masterm3u8\.m3u8" or die $!;
	my $m3u8="";
	foreach $line (@string) {
		$m3u8="";
		#if ($line=~/[\.ts|\.aac]/) {
			#print "rewrite $line\n";
			#if ($line=~/.*\/([\w\+\-\.m3u8]+)\??.*?/i) { $m3u8 = "$1";}
			if ($line=~/^((https?:\/\/.*\.akamaihd\.net\/i\/)(.*)\/([\w\+\-]+)_,.*,.mp4.csmil\/)?([\w\+\-\.m3u8]+)(\??.*)$/i) { $m3u8 = "$5";}
			my $lurl = "$line";
			chomp($lurl);
			if (!($m3u8 eq "")) {
					if ($debug)	{
						print "      | $newpath\_$m3u8\n";
					}						
					print OUTFILEMASTER "$newpath\_$m3u8\n";
				} else {
					if ($debug)	{
						print "      | $lurl\n";
					}
					print OUTFILEMASTER "$lurl\n";
				}
		#}
	}
	#print "\n-------------------------------------------------------------------------------------\n";
	close(OUTFILEMASTER);
}


#function to download segment
sub downloadSegment {
	my($tmpNSpath, $tmpTs, $tmpUrl, $tmpNSkey, $tmpNShost) = @_;
	my @cmd_out = `printf "cd $tmpNSpath \n wget --no-check-certificate --load-cookies ../cookies.txt --server-response -O $tmpTs '$tmpUrl'" | ssh -q -i $tmpNSkey sshacs\@$tmpNShost cms 2>&1`;
	return @cmd_out;
}

#add the Token
if (!$tokenKey eq "") {
	$TOKEN=getToken();
	if ($playlist=~/(\.m3u8\?)/i) {
		$playlist = "${playlist}&${TOKEN}";
	} else {
		$playlist = "${playlist}?${TOKEN}";
	}
	print CSVFILE "Generating Token,\"$TOKEN\",OK\n";
}
print INFOCOLOR,"\nDownloading Master Playlist: $playlist\n",RESET;

#variables dedicated to path and filename
my $url_baseurl = '';
my $url_hosturl = '';
my $url_path = '';
my $url_filename = '';
my $url_track = '';

### parse paths
print RESET,"Parsing paths based on stream url:\n",RESET;
if ($playlist=~/^((https?:\/\/.*\.akamaihd\.net\/i\/)(.*)\/([\w\+\-]+)_,.*,.mp4.csmil\/)?([\w\+\-]+)\.m3u8(\??.*)$/i)
{
	$url_baseurl = $1;
	$url_hosturl = $2;
	$url_path = $3.'/'.$4;
	$url_filename = $4;
	$url_track = $5;
	$netstorage_dir = $url_path;
	print "  base url : $url_baseurl\n";
	print "  host url : $url_hosturl\n";
	print "  path     : $url_path\n";
	print "  filename : $url_filename\n";
	print "  track    : $url_track\n";
}

### Clean up previous run
print "Cleaning up ./playlist & ./playlist_src directories ... ";
#sleep 1;
system ("rm -rdf ./playlist/*");
system ("rm -rdf ./playlist_src/*");
print "/ Flush directories Done\n";

# prepare a report of the download process
open CSVFILE, ">./$url_filename-$date\.csv" or die $!;
print CSVFILE "ACTION,URL,STATUS\n";

print DEBUGCOLOR,"",RESET;
#@master_m3u8 = `curl \"$playlist\" 2>/dev/null`;
my $masterurl = "./playlist_src/".$url_filename.".m3u8";
@master_m3u8 = `wget -v --keep-session-cookies --save-cookies ./cookies.txt -O $masterurl \"$playlist\" 2>&1`;
if ($debug)	{
	previewString(@master_m3u8);
}
if ( grep( /403 Forbidden/, @master_m3u8 ) ) {
  print ERRORCOLOR,"\nUnable to request the Master Playlist: error 403: Forbidden\n";
  print "Possible reason: content geo-restrited or a token is required, review your stream url.\n\n",RESET;
  exit(0);
}
# playlist is loaded, we continue...
print CSVFILE "Downloading Master Playlist,\"$playlist\",OK\n";
@master_m3u8 = loadFile($masterurl);
rewriteMasterM3U8($url_filename, $url_filename, @master_m3u8);
system ("cp ./cookies.txt ./playlist/");

if ($debug)	{
	print "\nMASTER PLAYLIST CONTENT \n @master_m3u8\n";
}

print "\nParsing Master playlist...\n";
my $i = 0;
my $bandwidth = 0;
foreach $line (@master_m3u8) {
	chomp($line);
	if ($line=~/^((https?:\/\/.*\.akamaihd\.net\/i\/)(.*)\/([\w\+\-]+)_,.*,.mp4.csmil\/)?([\w\+\-]+)\.m3u8(\??.*)$/i)
	{
		$url_hosturl = $2;
		$url_path = $3.'/'.$4;
		$url_track = $5;
		my $url_newPath = $url_filename.'_'.$url_track;
		my $url_newFilename = $url_filename.'_'.$url_track.'.m3u8';
		if ($debug)	{
			print "Found      : $line\n";
			print "  url      : $url_hosturl\n";
			print "  path     : $url_path\n";
			print "  filename : $url_filename\n";
			print "  track    : $url_track\n";
			print "  newPath    : $url_newPath\n";
			print "  newFilename    : $url_newFilename\n";
		}
		$i++;
		push @subplaylists, $line;
		print " + Found        $i: $url_newFilename ...\n";
		system ("mkdir ./playlist/$url_newPath");
	} else {
		# no match, debug?
		#print "no match : $line\n";
	}
}

print "\nProcessing Child Playlists ...\n";
foreach $subplaylist (@subplaylists) {
	$sub_num++;
	if ($subplaylist=~/^((https?:\/\/.*\.akamaihd\.net\/i\/)(.*)\/([\w\+\-]+)_,.*,.mp4.csmil\/)?([\w\+\-]+)\.m3u8(\??.*)$/i)
	{
		$url_hosturl = $2;
		$url_path = $3.'/'.$4;
		$url_track = $5;
		my $url_newPath = $url_filename.'_'.$url_track;
		my $url_newFilename = $url_filename.'_'.$url_track;
		if ($debug)	{
			print "Found      : $subplaylist\n";
			print "  url      : $url_hosturl\n";
			print "  path     : $url_path\n";
			print "  filename : $url_filename\n";
			print "  track    : $url_track\n";
			print "  newPath    : $url_newPath\n";
			print "  newFilename    : $url_newFilename\n";
    		print "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
		}
		$childdir = "$3";
		
		#case we have relative paths, not starting with http (from some customisation on SPOD delivery configurations on Akamai platform)
		if ($url_hosturl eq '') {
			$subplaylist = $url_baseurl.$subplaylist;
		}

		print INFOCOLOR, "Downloading Variant Playlist : $subplaylist ", RESET;
		my @child_m3u8;
		@child_m3u8 = `wget -v --no-check-certificate --load-cookies ./cookies.txt --server-response -O ./playlist_src/$url_newFilename.m3u8 \"$subplaylist\" 2>&1`;
		if ( grep( /200 OK/, @child_m3u8 ) ) {
			print GREEN," OK\n",RESET;
			print CSVFILE "Downloading Variant Playlist,\"$subplaylist\",OK\n";
			#exit(0);
		} else {
			print RED," ERROR\n",RESET;
			print "OUTPUT is $child_m3u8\n";
			print CSVFILE "Downloading Variant Playlist,\"$subplaylist\",ERROR\n";
			previewString(@child_m3u8);
			#exit(0);
		}
		@child_m3u8 = loadFile("./playlist_src/$url_newFilename.m3u8");
		rewriteChildM3U8($url_newFilename, @child_m3u8);
	}
}

# example data url to parse
#  base url : https://hostname-i.akamaihd.net/i/1176/11760097_,6,5,4,3,2,1,.mp4.csmil/
#  host url : https://hostname-i.akamaihd.net/i/
#  path     : 1176/11760097
#  filename : 11760097
#  track    : master


print "\nPreparing Netstorage... /$cp_code\/$netstorage_dir \n";

my @cmd_mk2 = `printf "mkdir -p \/$cp_code\/$netstorage_dir \n" | ssh -q -i $nsKey sshacs\@$netstorage_host cms`;
previewString(@cmd_mk2);
print CSVFILE "Uploading Playlists to Netstorage,\"$netstorage_host:/$cp_code\/$netstorage_dir\/\",OK\n";
print "\nExporting playlists to Netstorage... /$cp_code\/$netstorage_dir\/ \n";
system "scp -r -i $nsKey ./playlist/* sshacs\@$netstorage_host:/$cp_code/$netstorage_dir";

print "\nProcessing Segments ...\n";
foreach $subplaylist (@subplaylists) {
	$sub_num++;
	
	# subplaylist could be using the pattern /hls/live/streamID[-b]/path1/path2/rendition/filename.m3u8 or .akamaihd.net/i/streamname@streamID/filename.m3u8
	my @child_m3u8;
	if ( ($subplaylist=~/(https?:\/\/.*\.akamaihd\.net\/i\/)(.*)\/([\w\+\-]+)_,.*,.mp4.csmil\/([\w\+\-]+)\.m3u8/i) )
	{
		$url_hosturl = $1;
		$url_path = $2.'/'.$3;
		$url_filename = $3;
		$url_track = $4;
		$url_newFilename = $3.'_'.$4;
		if ($debug)	{
			print "Found      :\n";
			print "  url_baseurl      : $url_baseurl\n";
			print "  url      : $url_hosturl\n";
			print "  path     : $url_path\n";
			print "  filename : $url_filename\n";
			print "  track    : $url_track\n";
    		print "  url_newFilename    : $url_newFilename\n";
    		print "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
		}
		$childdir = "$3";

		if ($url_hosturl eq '') {
			$subplaylist = $url_baseurl.$subplaylist;
		}

		print RESET, "Parsing Variant Playlist : $subplaylist\n",RESET;
		@child_m3u8 = loadFile("./playlist_src/$url_newFilename.m3u8");
	}

	my $oktostart=1;
	$basepath = "";

	if ($oktostart eq 1) {
		foreach $line (@child_m3u8) {
			if ($line=~/\.ts/ || $line=~/\.aac/) {
				if ($line=~/^(.*\/)?([\w\+\-\.ts|\w\+\-\.aac]+)(\??.*)$/i) { $ts = "$2";}
				if ($line=~/^(http|https)\:\/\//i) {
					$url = "$basepath$line"; chomp($url);
				} else {
					$url = "$url_baseurl$line"; chomp($url);
				}
				
				print INFOCOLOR, "Downloading $url ", RESET;
				my @cmd_seg = downloadSegment("$cp_code/$netstorage_dir/$url_newFilename", $ts, $url, $nsKey, $netstorage_host);
				if ( grep( /200 OK/, @cmd_seg ) ) {
					print GREEN," OK\n",RESET;
					print CSVFILE "Downloading Segment,\"$url\",OK\n";
					#exit(0);
				} else {
					print RED," ERROR\n",RESET;
					print "OUTPUT is $cmd_seg\n";
					print CSVFILE "Downloading Segment,\"$url\",ERROR\n";
					previewString(@cmd_seg);
				}
			}
			else {
				#
			}
		}
		#
	}
}

print "\n\n";
close(CSVFILE);
print "Done!\n";

my $duration = time - $starttime;
my $durationMin = $duration/60;
	
print "$localTab\t- Process took $duration s = $durationMin min\n\n";
