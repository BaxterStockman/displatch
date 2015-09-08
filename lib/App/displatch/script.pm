#!/usr/bin/env perl

use strict;
use warnings;

use Carp ();
use Data::Dumper ();
use Getopt::Long qw(GetOptionsFromArray :config require_order);
use Scalar::Util ();

use subs qw(main _pipe _fork _open _close _debug);

use constant {
    BUFSIZE => 2048,
};

$SIG{PIPE} = sub {
    unshift @_, "Error in pipe()";
    goto &Carp::confess;
};

sub main {
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
        debug      => sub {
            my $debug = sub (@) {
                print STDERR "# DEBUG => @_\n" if -t STDERR;
            };

            {
                no strict 'refs';
                no warnings 'redefine';
                *_debug = $debug;
            }
        },
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
            _open my $fh, $input->[0], $input->[1];

            while (sysread($fh, $buf, BUFSIZE)) {
                syswrite($_, $buf) foreach @writers;
            }

            _close $fh;
        }
    }
    else {
        my $writer = $writers[0];
        foreach my $input ( @inputs ) {
            # Two-argument form so that '<-' works.
            _open my $fh, $input->[0], $input->[1];

            while (sysread($fh, $buf, BUFSIZE)) {
                syswrite($writer, $buf);
            }

            _close $fh;
        }
    }

    return 1;
}

sub run_pipeline {
    my ( @commands ) = @_;

    my %options = %{pop @commands} if ref $commands[$#commands] eq 'HASH';

    my $prev_reader;
    my $children_forked = 0;
    _pipe(my( $init_reader, $init_writer ));

    my $parent_handler = sub {
        my ( $reader, $writer ) = @_;

        # The writer will be used by the child process
        _close( $writer );

        # The reader will be passed to the child process created on the
        # next loop.
        $prev_reader = $reader;
    };

    my $child_handler = sub {
        my ( $reader, $writer, $command, $children_forked ) = @_;

        # Close reader; we'll be using the reader from the previous
        # pipe/fork combo.
        _close $reader;

        if ( $children_forked == 1 ) {
            # Open STDIN to previous reader
            _close \*STDIN;
            _open \*STDIN, '<&', \$init_reader;
        }
        else {
            # Open STDIN to previous reader
            _close \*STDIN;
            _open \*STDIN, '<&', \$prev_reader;
        }

        if ( $children_forked < @commands ) {
            # Open STDOUT to current writer
            _close \*STDOUT;
            _open \*STDOUT, '>&', \$writer;
        }

         _debug("exec $command");
        exec $command;
    };

    foreach my $command ( @commands ) {
        pipe_and_fork( $parent_handler, $child_handler, $command, ++$children_forked );
    }

    # From here on out, we're in the parent process.

    # Won't be needing this anymore
    _close( $init_reader );

    return $init_writer;
}

sub run_fanout {
    my ( @commands ) = @_;

    my %options = %{pop @commands} if ref $commands[$#commands] eq 'HASH';

    my $parent_handler = sub {
        my ( $reader, $writer ) = @_;

        # The reader will be used by the child process
        _close( $reader );

        return $writer;
    };

    my $child_handler = sub {
        my ( $reader, $writer, $command ) = @_;

        _close $writer;

        # Open STDIN to previous reader
        _close \*STDIN;
        _open \*STDIN, '<&', \$reader;


         _debug("exec $command");
        exec $command;
    };

    my @writers = $options{tee} ? \*STDOUT : ();
    foreach my $command ( @commands ) {
        push @writers, pipe_and_fork( $parent_handler, $child_handler, $command );
    }

    # From here on out, we're in the parent process.
    return @writers;
}

sub pipe_and_fork {
    my ( $parent_handler, $child_handler, @args ) = @_;

    _pipe(my( $reader, $writer ));

    my $handler = _fork() ? $parent_handler : $child_handler;

    return $handler->( $reader, $writer, @args );
}

sub _pipe ($$) {
    pipe( $_[0], $_[1] ) or die "Error from pipe(): $!";
}

sub _fork () {
    my $pid = fork;
    die "Error from fork(): $!" unless defined $pid;
    return $pid;
}

sub _open ($$;@) {
  open($_[0], $_[1], @_[2..$#_])
      or Carp::confess "Error from open(" . join(q{, }, @_) . "): $!";
}


sub _close ($) {
  close $_[0] or Carp::confess "Error from close(" . join(q{, }, @_) . "): $!";
}

sub _debug (@) {
    1;
}

# Begin main routine
exit ! main( @ARGV ) unless caller;
