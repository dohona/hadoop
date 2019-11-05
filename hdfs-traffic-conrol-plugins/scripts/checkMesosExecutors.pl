#!/usr/bin/perl -w
use strict;
use File::Basename;
use File::Path;
use Cwd;
use Cwd qw(chdir abs_path);
use Time::HiRes qw( gettimeofday tv_interval );
use File::stat;

my $mesosSlaveLog;
my $mesosMasterLog;
my $defaultRatePerTask;

use Getopt::Long;
&Getopt::Long::Configure("ignore_case");
&GetOptions( "rate=s"      => \$defaultRatePerTask);

$defaultRatePerTask = 100 unless $defaultRatePerTask;

my $flagLocation = "/tmp/monitoring_containers";

$| = 1;

my %appRates = (
	'JavaWordCount' => 30,
	'TeraSort'  => 10,
);

unless($mesosSlaveLog){
	if ($^O =~ /^(MS)?Win/) {
		$mesosSlaveLog = "g:\\SharedStorage\\Spark-wordcount\\lt-mesos-slave.INFO";
		$flagLocation = "g:\\SharedStorage\\Spark-wordcount";
	} else {
		$mesosSlaveLog = '/opt/logs/mesos/lt-mesos-slave.INFO';
		$mesosMasterLog = '/opt/logs/mesos/lt-mesos-master.INFO';
		$flagLocation = "/tmp/monitoring_containers";
	}
}

print "File to check: $mesosSlaveLog and $mesosMasterLog\n";

my $t_begin;

my $latestTime = 0;

my %printedFrameworks = ();
my %printedContainers = ();
my %printedPids = ();
my %hasChanges = ();
my %tasksNum = ();
my %frameworkType = ();

while(1){
	Time::HiRes::sleep(0.5);
	my $currTime = stat($mesosSlaveLog)->mtime;#[9];
	if($currTime > $latestTime) {
		$latestTime = $currTime;
	} else {
		next;
	}
	
	#$t_begin = [gettimeofday];
	#my $timestamp = localtime($currTime);          
	#printf "file %s updated at %s\n", $mesosLog, $timestamp;
	
	open( INFILE, "<$mesosSlaveLog" )
		or die "$mesosSlaveLog is not found $!";
	my @lines = <INFILE>;
	close INFILE;
	
	my %flagFiles = ();
	my %currentFrameworks = ();
	my %inProgressTasks = ();
	my %containerId;
	my %containerPid = ();
	my %tasks = ();
	
	for my $line (@lines){
		#next unless $line =~ /^I0915/;
		# I0914 18:39:16.024935  7850 slave.cpp:1355] Launching task 0 for framework 20150914-105640-1946159626-5050-15987-0019
		if($line =~ /slave.cpp:1355] Launching task (\d+) for framework (\S+)/){
			#print "task $1 for framework $2\n";
			$inProgressTasks{$2} = 0 unless $inProgressTasks{$2};
			$inProgressTasks{$2}++;
			$currentFrameworks{$2} = 1;
			$hasChanges{$2} = 1;
			push @{$tasks{$2}}, $1;
			#print "++inProgressTasks of framework $2 is $inProgressTasks{$2}\n";
			next;
		}
		
		# Only one executor/container for each framework one each slave!
		# containerizer.cpp:534] Starting container 'd1432a02-fbce-4a0d-8168-e5f73dc37e15' for executor '20150914-105640-1946159626-5050-15987-S2' of framework '20150914-105640-1946159626-5050-15987-0019'
		if($line =~ /containerizer.cpp:534] Starting container '(\S+)' for executor \S+ of framework '(\S+)'/) {
			#print "Container: $1 for framework $2\n";
			$containerId{$2} = $1;
			$flagFiles{$1} = $flagLocation . "/z_" . $1;
			$tasksNum{$2} = 0;
			next;
		}
		
		# launcher.cpp:131] Forked child with pid '15320' for container 'd1432a02-fbce-4a0d-8168-e5f73dc37e15'
		if($line =~ /launcher.cpp:131] Forked child with pid '(\d+)' for container '(\S+)'/) {
			#print "Pid of container: $2 is $1\n";
			$containerPid{$2} = $1;
			next;
		}
		
		# slave.cpp:2671] Handling status update TASK_FINISHED (UUID: 1360c191-7a41-4fee-91ea-789673945a0c) for task 0 of framework 20150914-105640-1946159626-5050-15987-0019
		if($line =~ /slave.cpp:2671] Handling status update TASK_FINISHED \(UUID: \S+\) for task (\d+) of framework (\S+)/) {
			$inProgressTasks{$2}--;
			$hasChanges{$2} = 1;
			#print "--inProgressTasks of framework $2 is $inProgressTasks{$2}\n";
			
			@{$tasks{$2}} = grep { $_ != $1 } @{$tasks{$2}};
			
			next;
		}
		
		# containerizer.cpp:1001] Destroying container 'fbd8bb5a-c9be-4661-896b-be51519e23e2'
		if($line =~ /containerizer.cpp:1001] Destroying container '(\S+)'/){
			delete $containerPid{$1};
			unlink $flagFiles{$1} if $flagFiles{$1};
			delete $flagFiles{$1};
			next;
		}
		
		# slave.cpp:3549] Cleaning up framework 20150914-105640-1946159626-5050-15987-0020
		if($line =~ /slave.cpp:3549] Cleaning up framework (\S+)/){
			delete $containerId{$1};
			delete $inProgressTasks{$1};
			delete $currentFrameworks{$1};
			next;
		}
	}
	
	for my $frameworkId (keys %currentFrameworks) {	 
		unless ($printedFrameworks{$frameworkId}){
			print time() ." - The framework $frameworkId is launched...\n";
			$printedFrameworks{$frameworkId} = 1;
		}
		
		my $cId = $containerId{$frameworkId};
		if($cId){
			unless($printedContainers{$cId}){
				print time() ." - containerId of $frameworkId is $cId\n";
				$printedContainers{$cId} = 1;
			}
			
			my $pid = $containerPid{$cId};
			unless($pid && $printedPids{$pid}){
				print time() ." - Pid of container of $frameworkId is $pid\n";
				$printedPids{$pid} = 1;
			}
		}
		
		if($hasChanges{$frameworkId}){
			print time() ." - inProgressTasksNum of $frameworkId is $inProgressTasks{$frameworkId}\n";
			print time() ." - Tasks: [" . join(",", @{$tasks{$frameworkId}}) . "]\n"
				if $inProgressTasks{$frameworkId};
			$hasChanges{$frameworkId} = 0;
			
			if($tasksNum{$frameworkId} != $inProgressTasks{$frameworkId} && $cId){
				
				loadFrameworkTypes() unless $frameworkType{$frameworkId};
				
				if($frameworkType{$frameworkId}){
					my $ratePerTask = getRatePerTaskApp($frameworkType{$frameworkId});
					my $flagFile = $flagFiles{$cId};
					print time() ." - Notify $flagFile\n";
					my $taskNum = $inProgressTasks{$frameworkId};
					$taskNum = 1 unless $taskNum;
					if($flagFile){
						open(DATA,">$flagFile") || die "Couldn't open file $flagFile, $!";
						print DATA sprintf("rate=%.2f\npid=%d\n", $taskNum * $ratePerTask, $containerPid{$cId});
						close DATA;	
					}
				}
			}
			$tasksNum{$frameworkId} = $inProgressTasks{$frameworkId};
		}
	}
	#printTime($t_begin);
}

sub printTime {
	my ($start_time) = (@_);
	my $elapsed = int( tv_interval ( $start_time ));
	my $e_hours = int($elapsed / 3600);
	my $e_minutes = int(($elapsed - 3600 * $e_hours) / 60);
	my $seconds = $elapsed - 3600 * $e_hours - 60 * $e_minutes;
	
	print("Total execution time is ");
	print($e_hours . "h". $e_minutes . "m" . $seconds . "s\n\n");
}


sub getRatePerTaskApp {
	my ($appType) = (@_);
	my $rate = $appRates{$appType};
	$rate = $defaultRatePerTask unless $rate;
	return $rate;
}

#Sending 1 offers to framework 35f727de-29b6-4b57-8292-3c50dda76f68-0001 (JavaWordCount)
sub loadFrameworkTypes {
	#Sending 1 offers to framework 35f727de-29b6-4b57-8292-3c50dda76f68-0001 (JavaWordCount)
	open( INFILE, "<$mesosMasterLog" )
		or die "$mesosMasterLog is not found $!";
	my @lines = <INFILE>;
	close INFILE;
	
	for my $line (@lines){
		#if($line =~ /Sending \d+ offers to framework (\S+) \((\w+)\)/){
		#	$frameworkType{$1} = $2 unless $frameworkType{$1};
		#} els
		if($line =~ /Registering framework (\S+) \((\w+)\)/) {
			$frameworkType{$1} = $2 unless $frameworkType{$1};
		}
	}
	
	for my $fid (keys %frameworkType){
		print "$fid is $frameworkType{$fid}\n";
	}
}
