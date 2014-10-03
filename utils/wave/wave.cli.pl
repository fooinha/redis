#!/usr/bin/perl


use strict;
use warnings;

use Data::Dumper;

use Getopt::Long;
use Term::ReadKey;
use IO::Handle;
use Redis;

#
# Command line and redis connection options and default values
#
my %redis_opts = (
   server    => 'localhost:6379',          # server name
   name      => 'wave-test',               # connection name
   debug     => 0,                         # debug requests/responses to stderr
   reconnect => 1                          # enables reconnection mode
);

my %wave_opts = (
   key         => undef,
   N           => 60,
   E           => 0.05,
   debug_lists => 0,
   auto_expire => 1
);


GetOptions (
   'server|s=s'     => \$redis_opts{server},
   'name|n=s'       => \$redis_opts{name},
   'debug|d!'       => \$redis_opts{debug},       # -nodebug or -nod disables debug
   'reconnect|r!'   => \$redis_opts{reconnect},   # -noreconnect or -nor disables reconnection
   'key|k=s'        => \$wave_opts{key},          # wave's key name
   'size|N=i'       => \$wave_opts{N},            # wave's window size
   'error|E=f'      => \$wave_opts{E},            # wave's error rate
   'list|l!'        => \$wave_opts{debug_lists},  # 
   'expire|x!'      => \$wave_opts{auto_expire},  # 

) or die("Error in command line arguments\n");

my %opts = ( %redis_opts, %wave_opts );

my $redis = Redis->new(
   %redis_opts
) or die ("Cannot create redis connection handle");


die("Please specify '--key' option ") if (not defined $wave_opts{key});


print STDERR "Connected to $opts{'server'}\n" if ($opts{debug});


autoflush STDOUT 1;
our $char = '';
my $key = $wave_opts{key};
my $n = $wave_opts{N};
my $e = $wave_opts{E};

print "-----------------------------\n";
print "* key => [$key] \n";
print "*   N => [$n]\n*   E => [$e] \n";
print "----- commands --------------\n";
print " - i ) increment wave by 1\n";
print " - g ) get wave correct total\n";
print " - e ) get wave fast total\n";
print " - t ) get wave full total\n";
print " - r ) reset wave\n";
print " - k ) redis keys\n";
print " - d ) debug wave\n";
print " - s ) save redis database\n";
print " - o ) debug redis object\n";
print " - f ) flush all keys\n";
print " - n ) timestamp now\n";
print "-----------------------------\n";
print "! press 'q' to quit\n> ";
ReadMode 5, *STDIN;


sub process_cmd {
   my ($reply, $error) = @_;

   if (defined $reply) {

      if (ref $reply eq 'ARRAY') {

         print "\n";

         foreach(@$reply) {
            print "$_\n";
         }

      } else {
         if ($char eq 'g' || $char eq 'i' || $char eq 'e' || $char eq 't') {
            print "TS: [". time. "] ";
         } 
         print "R: [$reply]";
      }
   }

   print "$error" if (defined $error);
}

sub enable {
   my $v = shift;
   return 'no' if (not defined $v);

   return 'yes' if ($v);
   return 'no';   
}


while(1){

   if (defined ($char = ReadKey(0)) ) {

      if (ord($char) == 10) {
         next;
      }

      # process key presses here

      ReadMode 0;
      if ($char eq 'g') {
         print " *GET SLOW* ";
         $redis->wvget( $key, 0, 'no', \&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 'e') {
         print " *GET FAST* ";
         $redis->wvget( $key, 0, 'yes', \&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 'o') {
         $redis->debug( 'object', $key, \&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 'd') {
         $redis->wvdebug( $key, &enable($wave_opts{debug_lists}), \&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 't') {
         $redis->wvtotal( $key, \&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 'f') {
         $redis->flushall(\&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 'k') {
         $redis->keys("*", \&process_cmd);
         $redis->wait_one_response();
      }

      if ($char eq 's') {
         $redis->save(\&process_cmd);
         $redis->wait_one_response();
      }


      if ($char eq 'i') {
         print " *INC* ";
         # "key [increment=0] [TIMESTAMP=now] [EXPIRE=yes] [WAVE-N=60] [WAVE-E=0.05] [wave-R=1024]"
         $redis->wvincrby( 
            $key, 1, time, 
            &enable($wave_opts{auto_expire}), 
            $n, $e,  
            \&process_cmd);

         $redis->wait_one_response();
      }

      if ($char eq 'r') {
         print " *RST* ";
         $redis->wvreset( $key, \&process_cmd);
         $redis->wait_one_response();
      }

      #  quit and say goodbye
      if($char eq 'q'){
         ReadMode 0, *STDIN;
         print " Bye !\n";
         exit 0;
      }

      if ($char eq 'n') {
         print "TS: [" . time(). "]";
      }

      # print "$char =>", ord($char);    

      print "\n> ";
      ReadMode 5;

      # if(length $char){exit}  # panic button on any key :-)
   }
}

1;

