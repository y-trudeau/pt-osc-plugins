package pt_online_schema_change_plugin;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;
use Data::Dumper;
local $Data::Dumper::Indent    = 1;
local $Data::Dumper::Sortkeys  = 1;
local $Data::Dumper::Quotekeys = 0;

sub new {
   my ($class, %args) = @_;
   my $self = { %args };
   return bless $self, $class;
}

sub init {
   my ($self, %args) = @_;
   print "PLUGIN: init()\n";
   $self->{orig_tbl} = $args{orig_tbl};
}

sub after_create_new_table {
   my ($self, %args) = @_;
   print "PLUGIN: after_create_new_table()\n";

   $self->{new_tbl} = $args{new_tbl};

   return;
}


sub before_create_triggers() {
    my ($self, %args) = @_;
    print "PLUGIN: before_create_triggers()\n";
    
    print "PLUGIN: checking if long running queries are running against the table\n";
    my $orig_tbl = $self->{orig_tbl}{name};    
    
    my ($tbl_name) = $orig_tbl =~ m/`[^`]*`\.`([^`]*)`/g;
    my $sql="select count(*) from information_schema.innodb_trx where trx_query "
        . "like '%" . $tbl_name . "%' and trx_started < now() - interval 5 second;";
    my $start_time = time();
    
    my $lock_count = 1;
    while ($lock_count > 0) {
        ($lock_count) = $self->{cxn}->dbh()->selectrow_array($sql);
        
        # Are we locked?
        if ($lock_count > 0) {
            print "PLUGIN: " . $lock_count . " query(ies) is running for more than 5s on the table, waiting.\n";
            sleep(5);
        }
        
        # Are we waiting on this for more than 2 min?
        if ( time() - $start_time > 120 ) {
            print "PLUGIN: waited for more than 2 minutes, giving up";
            exit 254;   # exit code 254 is only used here
        }
    }
}

sub before_swap_tables() {
    my ($self, %args) = @_;
    print "PLUGIN: before_swap_tables()\n";

    my $orig_tbl = $self->{orig_tbl}{name};    
    my $new_tbl = $self->{new_tbl}{name};
        
    # First, let's compare the tables to see if they are the same
    my $sql = "select (select count(*) from " . $orig_tbl . ") cntold, " 
        . "(select count(*) from " . $new_tbl . ") cntnew, " 
        . "(select count(*) from " . $orig_tbl . " natural left join " 
        . $new_tbl . ") cntjoinl, " 
        . "(select count(*) from " . $orig_tbl . " natural right join " 
        . $new_tbl . ") cntjoinr;";

    #print $sql . "\n";
    
    my ($cnt_old,$cnt_new,$cnt_joinl,$cnt_joinr) = $self->{cxn}->dbh()->selectrow_array($sql);
    
    if ($cnt_old != $cnt_new || $cnt_new != $cnt_joinl || $cnt_new != $cnt_joinr) {
        print "PLUGIN: Differences found between the old and new tables, aborting!\n";
        exit 252;  # Exit code 252 is only used here
    }
    
    print "PLUGIN: The old and new tables are found to be identical\n";

    # now, checking if something is running againt the old table
    my ($db_name,$orig_tbl_name) = $orig_tbl =~ m/`([^`]*)`\.`([^`]*)`/g;
    
    print "PLUGIN: checking if long running queries are running against the table\n";
    $sql="select count(*) from information_schema.innodb_trx where trx_query "
        . "like '%" . $orig_tbl_name . "%' and trx_started < now() - interval 5 second;";
    my $start_time = time();
    
    my $lock_count = 1;
    while ($lock_count > 0) {
        ($lock_count) = $self->{cxn}->dbh()->selectrow_array($sql);
        
        # Are we locked?
        if ($lock_count > 0) {
            print "PLUGIN: " . $lock_count . " query(ies) is running for more than 5s on the table, waiting.\n";
            sleep(5);
        }
        
        # Are we waiting on this for more than 2 min?
        if ( time() - $start_time > 120 ) {
            print "PLUGIN: waited for more than 2 minutes, giving up";
            exit 253;   # exit code 253 is only used here
        }
    }
    
}
1;
