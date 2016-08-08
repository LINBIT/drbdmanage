#!/usr/bin/env python2

HEADER = r"""
[31m #######  ######  ######  ######
[31m ##	# #	# #	# #	#[32m  #    #   ##	 #    #	  ##	 ####	#####
[31m ##	# #	# #	# #	#[32m  ##  ##  #  #	 ##   #	 #  #	#    #	#
[31m ##	# ######  ######  #	#[32m  # ## # #    # # #  #	#    #	#	#####
[31m ##	# #   #   #	# #	#[32m  #    # ###### #  # #	######	#  ###	#
[31m ##	# #    #  #	# #	#[32m  #    # #    # #   ##	#    #	#    #	#
[31m #######  #	# ######  ######[32m   #    # #    # #    #	#    #	 ####	######[0m



+;
# Mind the tabs above!


sub BEGIN {
  eval q{ use JSON; };
  print("Got an error $@.\n\n
Maybe you need to install JSON?\n
   apt-get install libjson-perl\n
   yum install perl-JSON\n"), exit if $@;
}


my $bg = shift() || "light";

my $c1 = `tput setaf 1`;
my $c2 = `tput setaf 2`;
my $c0 = `tput setaf 6`;
my $cr = `tput sgr0`;
my $nm = `tput snrmq`;
my $bl = `tput bold`;

$nodelen = 20;
$reslen = 24;

# gather data
$/="\0";
*DATA=*STDIN if -b STDIN or -f STDIN;
our $idx_o;
$idx_o = Data(0, 0x1800, 4096);

my $n_o = Data('nodes');
my $r_o = Data('res');
my $a_o = Data('assg');
my $C_o = Data('cconf');


my $idx_o2 = $idx_o->{"index"};
my ($space_used) = sort { $b <=> $a } map {
       my $n = $_;
       $n =~ s/_\w+?$//;
       $idx_o2->{$n . "_off"} + $idx_o2->{$n . "_len"};
} keys %{$idx_o->{"index"}};
my $space_used_in_ctrl = substr(sprintf("==========ctrl-used:%s", Size($space_used)), -18);


my @node = map {
	/^(.+?)\./;
	# remove part before first dot
	my $n = ($1 || $_);
	my $n2 = $n_o->{$_}{_addr};
	# remove first octets of IP if too long
	1 while length($n . $n2) > $nodelen-2 && $n2 =~ s/\.?\d+\./$1./;
	$n . $c0 . "(" . $c2 . $n2 . $c0 . ")" . $c0;
	$n . "(" . $n2 . ")";
	my $n_len =  $nodelen - 2 - length($n2) - 1;
	sprintf("%s%-*.*s %s(%s%s%s)",
			$c2, $n_len, $n_len, $n, $c0,
			$c2, $n2, $c0);
} (sort keys %$n_o);

$node[$_] = " " x $nodelen for (@node .. 9);
$node[9] = sprintf("%-*s", $nodelen, " ... ") if $node[10];

my $string = " @node "; # needed for perl-5.10.1-136.el6.i686; else the node list is just "hash"


my %res = map {
	my $s = 0;
	$s += $_->{"_size_kiB"} for (values %{$r_o->{$_}{'volumes'}});
	($_, [$s, 0]);
} (sort keys %$r_o);

for (sort keys %$a_o) {
	my($h, $r) = split(/:/, $_, 2);
	$res{$r}[1]++;
}


my @res_l2 = map {
	sprintf("%s %-13.13s %s%d*%s(%s%4s%s)%s ",
            $c2, $_,
            $c1, $res{$_}[1],
            $c0, $c2, Size($res{$_}[0]), $c0,
            $c0);
} sort {
	$res{$b}[1] <=> $res{$a}[1] ||
	$res{$b}[0] <=> $res{$a}[0] ||
	$a cmp $b;
} keys %res;
$res_l2[$_] = " " x $reslen for (@res_l2 .. 19);
$res_l2[19] = sprintf("%-*s", $reslen, " ... ") if $res_l2[20];


my $serial = "Ser#" . $C_o->{"serial"};
my $serial_string = ("=" x (14 - length($serial))) . $serial;


$DRBDmgr =~ s/^.*\n//;
$DRBDmgr =~ s/\n$//;

print "\n",$DRBDmgr,<<"EOF";
$c0 ....|$c1 Nodes $c0|..........   ....|$c1 Resources $c0|........_........................
 : $node[0] |  :@res_l2[0]:@res_l2[10]|
 : $node[1] |  :@res_l2[1]:@res_l2[11]|
 : $node[2] |  :@res_l2[2]:@res_l2[12]|
 : $node[3] |  :@res_l2[3]:@res_l2[13]|
 : $node[4] |  :@res_l2[4]:@res_l2[14]|
 : $node[5] |  :@res_l2[5]:@res_l2[15]|
 : $node[6] |  :@res_l2[6]:@res_l2[16]|
 : $node[7] |  :@res_l2[7]:@res_l2[17]|
 : $node[8] |  :@res_l2[8]:@res_l2[18]|
 : $node[9] |  :@res_l2[9]:@res_l2[19]|
  ==$space_used_in_ctrl=+   ========================*========$serial_string==+$cr

EOF
exit;

sub Size
{
	my($s) = @_;
	my $unit = "EPTGMK";

	chop($unit), $s /= 1024 while $s >= 1024;

	my $u = chop($unit) || "?";
	return sprintf("%3d%s", $s, $u) if $s >= 10;

	my $sz = sprintf("%.1f%s", $s);
	return $sz . $u;
}

sub Data
{
	my($name, $offset, $len) = @_;
	my $i;

	seek(DATA, $offset // $idx_o->{'index'}{$name . "_off"}, 0) or die $!;
	read(DATA, $i, $len // $idx_o->{'index'}{$name . "_len"}) or die $!;

	return from_json($i, {utf8 => 1});
}

# some spaces here, please
#                                 #
__DATA__

"""


def gen_header():
    import uuid
    ctrlvoluuid = uuid.uuid4()
    return "$DRBDmgr=q+%s" % (str(ctrlvoluuid).replace('-', '')) + HEADER
