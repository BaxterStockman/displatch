use strict;
use warnings;

package Displatch::App;

use FindBin;
use lib "$FindBin::Bin/..";

use Displatch qw(run_pipeline run_fanout);
use Getopt::Long qw(GetOptionsFromArray :config require_order);

use Exporter qw(import);
our @EXPORT_OK = qw(run);

use constant {
    BUFSIZE => 2048,
};

$SIG{PIPE} = sub {
    unshift @_, "Error in pipe()";
    goto &Carp::confess;
};

sub run {
    my ( @args ) = @_;

    #my @commands;
    my $operation;
    my %options = (
        tee         => undef,
        pipeline    => sub {
            $operation = \&run_pipeline;
        },
        fanout      => sub {
            $operation = \&run_fanout;
        },
        debug      => \$Displatch::Debug,
    );

    GetOptionsFromArray(
        \@args,
        \%options,
        'tee',
        'fanout',
        'pipeline',
        'debug',
    ) or die "Error in options\n";

    my $separator = '--';
    my $separator_index = 0;
    foreach my $arg ( @args ) {
        last if $arg eq $separator;
        $separator_index++;
    }

    # Commands are everything until just before '--'
    my @commands = splice @args, 0, $separator_index;
    # And inputs are everything afterward.  Default to opening STDIN if there
    # are no other inputs.
    my @inputs = $#args ? map { ['<', $_] } @args[1..$#args] : ();
    unless ( @inputs ) {
        if ( -t STDIN ) {
            die "No input provided";
        }
        push @inputs, ['<&', \*STDIN];
    }

    my @writers = ($operation // \&run_pipeline)->( @commands, \%options );

    my $buf;
    if ( scalar @writers > 1 ) {
        foreach my $input ( @inputs ) {
            Displatch::_open my $fh, $input->[0], $input->[1];

            while (sysread($fh, $buf, BUFSIZE)) {
                syswrite($_, $buf) foreach @writers;
            }

            Displatch::_close $fh;
        }
    }
    else {
        my $writer = $writers[0];
        foreach my $input ( @inputs ) {
            # Two-argument form so that '<-' works.
            Displatch::_open my $fh, $input->[0], $input->[1];

            while (sysread($fh, $buf, BUFSIZE)) {
                syswrite($writer, $buf);
            }

            Displatch::_close $fh;
        }
    }

    return 1;
}

1;
