use strict;
use warnings;

package Displatch;

use Carp ();
use Getopt::Long qw(GetOptionsFromArray :config require_order);

use subs qw(_pipe _fork _open _close _debug);

use Exporter ();
our @EXPORT_OK = qw(
    run_pipeline
    run_fanout
);
our %EXPORT_TAGS = (
    all => \@EXPORT_OK,
);

{
    our $Debug = 0;
}

sub import {
    my ( $class, @imports ) = @_;

    my @passthrough_imports = grep { $_ ne ':debug' } @imports;

    our $Debug = 1 if @passthrough_imports = @imports;

    return Exporter::import( $class, @passthrough_imports );
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
    print STDERR "# DEBUG => @_\n" if (-t STDERR and our $Debug);
}

1;
