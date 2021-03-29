#!/usr/bin/perl

use Data::Dumper;
use strict;

if (!$ARGV[0]) {
	print("Usage: perl recovery.pl file.sql\n");
	exit 1;
}

my %inserts = ();
my %updates = ();
my %ids = {};

sub assemble_query {
	my $hash = $_[0];
	my $key = $_[1];
    my $idt_old = ${$hash}{$key};
    my $idt_new = $ids{$idt_old};
	if (!$idt_new) {
		$idt_new = $idt_old;
	}
	print("idt_antigo => " . $idt_old . "\n");
	print("idt_novo => " . $idt_new . "\n");
	my $value = ${$hash}{'query'};
	$value =~ s/$idt_old/$idt_new/;
	print($value . "\n");
}

sub assemble_query_update {
	my $hash = $_[0];
    my $idt_old = ${$hash}{'old_idt'};
    my $idt_new = $ids{$idt_old};
	if (!$idt_new) {
		$idt_new = $idt_old;
	}
	print("idt_antigo => " . $idt_old . "\n");
	print("idt_novo => " . $idt_new . "\n");
	my $value = ${$hash}{'query'};
	$value =~ s/IDT_CLIENT=$idt_old/IDT_CLIENT=$idt_new/;
	print($value . "\n");
}

my $sql = open(my $fh, '<:encoding(UTF-8)', $ARGV[0]) or die $!;
while(<$fh>) {
	if($_ =~ /INSERT_ID/) {
		my ($id) = $_ =~ /(\d+)/;
		while(<$fh>) {
			if ($_ =~ /insert into /) {
				my ($tabela) = $_ =~ /^insert into ([\w]+) \(/;
				if ($tabela eq 'CLIENT') {
					my ($cod) = $_ =~ /(\d{6,})/;
					if(!$inserts{$tabela}) {
						$inserts{$tabela} = ();
					}
					my $new_id = int($id) + 10000000;
					my %entry = ('id', $id, 'cod', $cod, 'id_new', $new_id, 'query', $_);
					push(@{$inserts{$tabela}}, \%entry);
					$ids{$id} = $new_id;
				} elsif ($tabela eq 'PLAN_CLIENT') {
					if(!$inserts{$tabela}) {
						$inserts{$tabela} = ();
					}
					my ($old_idt) = $_ =~ /(\d{6,})/;
					my %entry = ('id', $id, 'cliente', $old_idt, 'query', $_);
					push(@{$inserts{$tabela}}, \%entry);
				} else {
					my $delimiter = '(\d{4,}),';
					if ($tabela eq 'CLIENT_PLAN_CHANGE_AUDIT') {
						$delimiter = ', (\d+),';
					}
					my ($cod) = $_ =~ /$delimiter/;
					if(!$inserts{$tabela}) {
						$inserts{$tabela} = ();
					}
					my %entry = ('id', $id, 'cliente', $cod, 'query', $_);
					push(@{$inserts{$tabela}}, \%entry);
				}
				last;
			} elsif ($_ =~ /update /) {
				print('Nenhum update');
				# Nenhum update
			}
		}
		my $pos = 0;
		while(<$fh>) {
			if ($_ !~ /INSERT_ID/) {
				if($_ =~ /insert into /) {
					# CLIENT_DETAIL
					my ($tabela) = $_ =~ /^insert into ([\w]+) \(/;
					my $delimiter = ', (\d+)\)';

					my ($cod) = $_ =~ /, (\d+)\)/;
					if(!$inserts{$tabela}) {
						$inserts{$tabela} = ();
					}
					my %entry = ('id', $id, 'cliente', $cod, 'query', $_);
					push(@{$inserts{$tabela}}, \%entry);
				} elsif ($_ =~ /update /) {
					my ($tabela) = $_ =~ /^update ([\w]+) /;
					if(!$updates{$tabela}) {
						$updates{$tabela} = ();
					}
					my ($idt_client) = $_ =~ /IDT_CLIENT=(\d+)/;
					my %entry = ('old_idt', $idt_client, 'query', $_);
					push(@{$updates{$tabela}}, \%entry);
				}
				$pos = tell $fh;
			} else {
				seek $fh, $pos, 0;
				last;
			}
		}
	}
}

#print "$_\n" for keys %updates;
#exit;
#print Dumper(\%updates);
#exit;
#print Dumper(\%inserts{'CONTRACT_CLIENT'});
#exit;

foreach(@{$inserts{'CLIENT'}}) {
	my $idt_old = ${$_}{'id'};
	my $idt_new = ${$_}{'id_new'};
	if(!$idt_new) {
		$idt_new = $idt_old;
	}
	print("idt_antigo => " . $idt_old . "\n");
	print("idt_novo => " . $idt_new . "\n");
	my $value = ${$_}{'query'};
	$value =~ s/COD_CLIENT/IDT_CLIENT, COD_CLIENT/;
	$value =~ s/values \(/values ($idt_new, /;
	$value =~ s/$idt_old/$idt_new/;
	print($value . "\n");
}
foreach(@{$inserts{'CLIENT_DETAIL'}}) {
	assemble_query($_, 'id')
}
foreach(@{$inserts{'CONTRACT_CLIENT'}}) {
	#assemble_query($_, 'cliente');
	my $id = ${$_}{'id'};
    my $idt_old = ${$_}{'cliente'};
    my $idt_new = $ids{$idt_old};
	if (!$idt_new) {
		$idt_new = $idt_old;
	}
	print("idt_antigo => " . $idt_old . "\n");
	print("idt_novo => " . $idt_new . "\n");
	my $value = ${$_}{'query'};
	$value =~ s/IDT_CALLER/IDT_CONTRACT_CLIENT, IDT_CALLER/;
	$value =~ s/values \(/values ($id, /;
	$value =~ s/, $idt_old,/, $idt_new,/;
	print($value . "\n");
}
foreach(@{$inserts{'PLAN_CLIENT'}}) {
	my $id = ${$_}{'id'};
    my $idt_old = ${$_}{'cliente'};
    my $idt_new = $ids{$idt_old};
	if (!$idt_new) {
		$idt_new = $idt_old;
	}
	print("idt_antigo => " . $idt_old . "\n");
	print("idt_novo => " . $idt_new . "\n");
	my $value = ${$_}{'query'};
	$value =~ s/IDT_CLIENT/IDT_PLAN_CLIENT, IDT_CLIENT/;
	$value =~ s/values \(/values ($id, /;
	$value =~ s/, $idt_old,/, $idt_new,/;
	print($value . "\n");
}
foreach(@{$inserts{'CLIENT_PLAN_CHANGE_AUDIT'}}) {
	assemble_query($_, 'cliente');
}
foreach(%updates) {
	my $tabela = $_;
	foreach(@{$updates{$tabela}}) {
		assemble_query_update($_);
	}
}

# - Updates
# CLIENT_DETAIL
# PLAN_CLIENT
# CONTRACT_CLIENT

# - Inserts
# INSTALLMENT
# PLAN
# COST
# CLIENT_PLAN_CHANGE_AUDIT
# CLIENT_DETAIL
# PLAN_CLIENT
# CLIENT
# ESCROW
# CONTRACT_CLIENT

close($fh);
