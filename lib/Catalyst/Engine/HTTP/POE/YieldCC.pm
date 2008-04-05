package Catalyst::Engine::HTTP::POE::YieldCC;

use strict;
use warnings;
use base 'Catalyst::Engine::HTTP';
use Data::Dump;
use HTTP::Body;
use HTTP::Status ();
use POE;
use POE::Driver::SysRW;
use POE::Filter::Stream;
use POE::Session::YieldCC;
use POE::Wheel::ReadWrite;
use POE::Wheel::SocketFactory;
use Socket;

use Catalyst::Engine::HTTP::Restarter::Watcher;

our $VERSION = '0.06';

# Enable for helpful debugging information
sub DEBUG { $ENV{CATALYST_POE_DEBUG} || 0 }

# sysread block size
sub BLOCK_SIZE { 4096 }

# Max processes (including parent)
sub MAX_PROC { $ENV{CATALYST_POE_MAX_PROC} || 1 }

sub run { 
    my ( $self, $class, @args ) = @_;
    
    $self->spawn( $class, @args );
    
    POE::Kernel->run;
}

sub spawn {
    my ( $self, $class, $port, $host, $options ) = @_;
    
    my $addr = $host ? inet_aton($host) : INADDR_ANY;
    if ( $addr eq INADDR_ANY ) {
        require Sys::Hostname;
        $host = lc Sys::Hostname::hostname();
    }
    else {
        $host = gethostbyaddr( $addr, AF_INET ) || inet_ntoa($addr);
    }
    
    $self->{config} = {
        appclass   => $class,
        addr       => $addr,
        port       => $port,
        host       => $host,
        options    => $options,
        children   => {},
        is_a_child => 0,
    };
    
    POE::Session::YieldCC->create(
        object_states => [
            $self => [
                qw/_start
                   _stop
                   shutdown
                   child_shutdown
                   dump_state
                   
                   prefork
                   sig_chld
                   
                   check_restart
                   restart
                   
                   accept_new_client
                   accept_failed

                   client_flushed
                   client_error
                   
                   read_headers
                   parse_headers
                   process

                   handle_prepare
                   read_body_chunk

                   handle_finalize
               /
           ],
       ],
   );
   
   return $self;
}

# start the server
sub _start {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];
    
    $kernel->alias_set( 'catalyst-poe' );

    # take a copy of %ENV
    $self->{global_env} = \%ENV;
    
    $self->{listener} = POE::Wheel::SocketFactory->new(
         ( defined ( $self->{config}->{addr} ) 
            ? ( BindAddress => $self->{config}->{addr} ) 
            : () 
         ),
         ( defined ( $self->{config}->{port} ) 
            ? ( BindPort => $self->{config}->{port} ) 
            : ( BindPort => 3000 ) 
         ),
         SuccessEvent   => 'accept_new_client',
         FailureEvent   => 'accept_failed',
         SocketDomain   => AF_INET,
         SocketType     => SOCK_STREAM,
         SocketProtocol => 'tcp',
         Reuse          => 'on',
    );

    # dump our state if we get a SIGUSR1
    $kernel->sig( USR1 => 'dump_state' );

    # shutdown on INT
    $kernel->sig( INT => 'shutdown' );
    
    # Pre-fork if requested
    $self->{config}->{options}->{max_proc} ||= MAX_PROC;
    if ( $self->{config}->{options}->{max_proc} > 1 ) {
        $kernel->sig( CHLD => 'sig_chld' );
        $kernel->yield( 'prefork' );
    }
    
    # Init restarter
    if ( $self->{config}->{options}->{restart} ) {
        my $delay = $self->{config}->{options}->{restart_delay} || 1;
        $kernel->delay_set( 'check_restart', $delay );
    }
    
    my $url = 'http://' . $self->{config}->{host};
    $url .= ':' . $self->{config}->{port}
        unless $self->{config}->{port} == 80;

    print "You can connect to your server at $url\n";
}

sub _stop { }

sub shutdown {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];
    
    DEBUG && warn "Shutting down...\n";
    
    if ( my @children = keys %{ $self->{config}->{children} } ) {
        DEBUG && warn "Signaling all children to stop...\n";
        kill INT => @children;
    }
    
    delete $self->{listener};
    delete $self->{clients};
    
    $kernel->alias_remove( 'catalyst-poe' );
    
    return 1;
}

sub child_shutdown {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];

    return 0;
}

sub dump_state {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];

    my $clients = scalar keys %{ $self->{clients} };
    warn "-- POE Engine State --\n";
    warn Data::Dump::dump( $self );
    warn "Active clients: $clients\n";
}

sub prefork {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];
    
    return if $self->{config}->{is_a_child};
    
    my $max_proc = $self->{config}->{options}->{max_proc};
    
    DEBUG && warn 'Preforking ' . ( $max_proc - 1 ) . " children...\n";
    
    my $current_children = keys %{ $self->{config}->{children} };
    for ( $current_children + 2 .. $max_proc ) {
        
        my $pid = fork();
        
        unless ( defined $pid ) {
            DEBUG && warn "Server $$ fork failed: $!\n";
            $kernel->delay_set( prefork => 1 );
            return;
        }
        
        # Parent.  Add the child process to its list.
        if ( $pid ) {
            $self->{config}->{children}->{$pid} = 1;
            next;
        }
        
        # Child.  Clear the child process list.
        DEBUG && warn "Child $$ forked successfully.\n";
        $self->{config}->{is_a_child} = 1;
        $self->{config}->{children}   = {};
        
        $kernel->sig( INT => 'child_shutdown' );
        
        return;
    }
}

sub sig_chld {
    my ( $kernel, $self, $child_pid ) = @_[ KERNEL, OBJECT, ARG1 ];

    if ( delete $self->{config}->{children}->{$child_pid} ) {
        DEBUG && warn "Server $$ received SIGCHLD from $child_pid.\n";
        $kernel->yield( 'prefork' );
    }
    return 0;
}

sub check_restart {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];
    
    # Only check in the parent process
    return if $self->{config}->{is_a_child};
    
    my $options = $self->{config}->{options};
    
    # Init watcher object with no delay
    if ( !$self->{watcher} ) {        
        $self->{watcher} = Catalyst::Engine::HTTP::Restarter::Watcher->new(
            directory => ( 
                $options->{restart_directory} || 
                File::Spec->catdir( $FindBin::Bin, '..' )
            ),
            regex     => $options->{restart_regex},
            # current Cat versions will 'sleep 1' if this is 0
            delay     => 0.00000000001,
        );
    }
    
    my @changed_files = $self->{watcher}->watch();
    
    # Restart if any files have changed
    if (@changed_files) {
        my $files = join ', ', @changed_files;
        print STDERR qq/File(s) "$files" modified, restarting\n\n/;
        
        $kernel->yield( 'restart' );
    }
    else {
        # Schedule next check
        $kernel->delay_set( 'check_restart', $options->{restart_delay} );
    }
}

sub restart {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];
    
    $kernel->call( 'catalyst-poe', 'shutdown' );
    
    ### if the standalone server was invoked with perl -I .. we will loose
    ### those include dirs upon re-exec. So add them to PERL5LIB, so they
    ### are available again for the exec'ed process --kane
    use Config;
    $ENV{PERL5LIB} .= join $Config{path_sep}, @INC;
    
    my $options = $self->{config}->{options};
    exec $^X . ' "' . $0 . '" ' . join( ' ', @{ $options->{argv} } );
}

sub accept_new_client {
    my ( $kernel, $self, $socket, $peeraddr, $peerport ) 
        = @_[ KERNEL, OBJECT, ARG0 .. ARG2 ];

    $peeraddr = inet_ntoa($peeraddr);
    
    my $wheel = POE::Wheel::ReadWrite->new(
        Handle       => $socket,
        OutputFilter => POE::Filter::Stream->new,
        FlushedEvent => 'client_flushed',
        ErrorEvent   => 'client_error',
        HighMark     => 128 * 1024,
        HighEvent    => sub {}, # useless, never gets called
        LowMark      => 8 * 1024,
        LowEvent     => sub {}, # also useless, we can use FlushedEvent
    );

    # get the local connection information
    my $local_sockaddr = getsockname($socket);
    my ( undef, $localiaddr ) = sockaddr_in($local_sockaddr);
    my $localaddr = inet_ntoa($localiaddr) || '127.0.0.1';
    my $localname = gethostbyaddr( $localiaddr, AF_INET ) || 'localhost';
    
    my $ID = $wheel->ID;
    
    $self->{clients}->{$ID}->{wheel}     = $wheel;
    $self->{clients}->{$ID}->{peeraddr}  = $peeraddr;
    $self->{clients}->{$ID}->{peerport}  = $peerport;
    $self->{clients}->{$ID}->{localaddr} = $localaddr;
    $self->{clients}->{$ID}->{localname} = $localname;

    # Use a SysRW driver for better input control than we can get from a Wheel
    $self->{clients}->{$ID}->{driver}    = POE::Driver::SysRW->new;
    $self->{clients}->{$ID}->{socket}    = $socket;
    
    DEBUG && warn "[$ID] [$$] New connection\n";

    # Wait for some data to read
    $self->{clients}->{$ID}->{ibuf} = '';
    $poe_kernel->select_read( $socket, 'read_headers', $ID );
}

sub accept_failed {
    my ( $kernel, $self ) = @_[ KERNEL, OBJECT ];
    
    $kernel->yield('shutdown');
}

sub client_error {
    my ( $kernel, $self, $ID ) = @_[ KERNEL, OBJECT, ARG3 ];
    
    my $op     = $_[ ARG0 ];
    my $errnum = $_[ ARG1 ];
    my $errstr = $_[ ARG2 ];
    
    DEBUG && warn "[$ID] [$$] Wheel generated $op error $errnum: $errstr\n";
    
    delete $self->{clients}->{$ID};
}

sub read_headers {
    my ( $kernel, $self, $handle, $ID ) = @_[ KERNEL, OBJECT, ARG0, ARG2 ];

    my $client = $self->{clients}->{$ID};

    my $line = $self->_get_line( $handle );

    DEBUG && warn "[$ID] [$$] Buffering input: $line\n";
    $client->{ibuf} .= $line . "\n";

    if ( $line eq '' ) {
        # Headers done
        $kernel->select_read( $handle );
        $kernel->yield( 'parse_headers', $ID );
    }
}

sub parse_headers {
    my ( $kernel, $self, $ID ) = @_[ KERNEL, OBJECT, ARG0 ];

    my $client = $self->{clients}->{$ID};

    DEBUG && warn "[$ID] [$$] parse_headers\n";

    my @lines = split /\n/, delete $client->{ibuf};

    # parse the request line
    my $line = shift @lines;
    my ( $method, $uri, $protocol ) =
        $line =~ m/\A(\w+)\s+(\S+)(?:\s+HTTP\/(\d+(?:\.\d+)?))?\z/;
        
    # Initialize CGI environment
    my ( $path, $query_string ) = split /\?/, $uri, 2;
    my %env = (
        PATH_INFO       => $path         || '',
        QUERY_STRING    => $query_string || '',
        REMOTE_ADDR     => $client->{peeraddr},
        REMOTE_HOST     => $client->{peeraddr},
        REQUEST_METHOD  => $method || '',
        SERVER_NAME     => $client->{localname},
        SERVER_PORT     => $self->{config}->{port},
        SERVER_PROTOCOL => "HTTP/$protocol",
        %{ $self->{global_env} },
    );
    
    for $line ( @lines ) {
        last if $line eq '';
        next unless my ( $name, $value ) 
            = $line =~ m/\A(\w(?:-?\w+)*):\s(.+)\z/;

        $name = uc $name;
        $name = 'COOKIE' if $name eq 'COOKIES';
        $name =~ tr/-/_/;
        $name = 'HTTP_' . $name
          unless $name =~ m/\A(?:CONTENT_(?:LENGTH|TYPE)|COOKIE)\z/;
        if ( exists $env{$name} ) {
            $env{$name} .= "; $value";
        }
        else {
            $env{$name} = $value;
        }
    }
    
    $client->{env} = \%env;
    
    $kernel->yield( 'process', $ID );
}

sub process {
    my ( $kernel, $self, $ID ) = @_[ KERNEL, OBJECT, ARG0 ];

    my $class = $self->{config}->{appclass};

    # This request may be executing within another request,
    # so we must localize all of NEXT so it doesn't get confused about what's
    # already been called
    local $NEXT::NEXT{ $class, 'prepare' };
    local $NEXT::NEXT{ $class, 'prepare_request' };
    local $NEXT::NEXT{ $class, 'prepare_connection' };
    local $NEXT::NEXT{ $class, 'prepare_query_parameters' };
    local $NEXT::NEXT{ $class, 'prepare_headers' };
    local $NEXT::NEXT{ $class, 'prepare_cookies' };
    local $NEXT::NEXT{ $class, 'prepare_path' };
    local $NEXT::NEXT{ $class, 'prepare_body' };

    local $NEXT::NEXT{ $class, 'finalize_uploads' };
    local $NEXT::NEXT{ $class, 'finalize_error' };
    local $NEXT::NEXT{ $class, 'finalize_headers' };
    local $NEXT::NEXT{ $class, 'finalize_body' };
    
    # pass flow control to Catalyst
    my $status = $class->handle_request( $ID );
}

# Prepare handles the entire prepare stage so we can yield to each step
sub prepare {
    my ( $self, $c, $ID ) = @_;

    DEBUG && warn "[$ID] [$$] - prepare\n";

    # store our ID in context
    $c->{_POE_ID} = $ID;
    
    my $client = $self->{clients}->{$ID};
    $client->{context} = $c;
    
    $poe_kernel->yield( 'handle_prepare', 'prepare_request', $ID );
    $poe_kernel->yield( 'handle_prepare', 'prepare_connection', $ID );
    $poe_kernel->yield( 'handle_prepare', 'prepare_query_parameters', $ID );
    $poe_kernel->yield( 'handle_prepare', 'prepare_headers', $ID );
    $poe_kernel->yield( 'handle_prepare', 'prepare_cookies', $ID );
    $poe_kernel->yield( 'handle_prepare', 'prepare_path', $ID );
    
    if ( !$c->config->{parse_on_demand} ) {
        $poe_kernel->yield( 'handle_prepare', 'prepare_body', $ID );
        # prepare_body will call prepare_done after reading all data
    }
    
    # On-demand parsing will call prepare_done after prepare_path is processed

    # Wait until all prepare processing has completed, or we will return too
    # early
    $poe_kernel->get_active_session->wait( "prepare_done_$ID" );

    return $c;
}

# handle_prepare localizes our per-client %ENV and calls $c->$method
# Allows plugins to do things during each step
sub handle_prepare {
    my ( $kernel, $self, $method, $ID ) = @_[ KERNEL, OBJECT, ARG0, ARG1 ];
    
    DEBUG && warn "[$ID] [$$] - $method\n";
    
    my $client = $self->{clients}->{$ID};
    
    {
        local (*ENV) = $client->{env};
        $client->{context}->$method();
    }
    
    # If on-demand parsing is enabled, we are done preparing after prepare_path
    if (   $client->{context}->config->{parse_on_demand}
        && $method eq 'prepare_path'
    ) {
        $kernel->yield( "prepare_done_$ID" );
    }
}

sub prepare_body {
    my ( $self, $c ) = @_;

    my $ID = $c->{_POE_ID};
    my $client = $self->{clients}->{$ID};
    
    # Initialize the HTTP::Body object
    my $type   = $c->request->header('Content-Type');
    my $length = $c->request->header('Content-Length') || 0;

    unless ( $c->request->{_body} ) {
        $c->request->{_body} = HTTP::Body->new( $type, $length );
    }

    if ( !$length ) {
        # Nothing to parse, we're done
        $poe_kernel->yield( "prepare_done_$ID" );
        return;
    }
   
    DEBUG && warn "[$ID] [$$] Starting to read POST data (length: $length)\n";

    # set block size to read
    my $driver = $client->{driver};
    $driver->[ $driver->BLOCK_SIZE ] = BLOCK_SIZE;

    # Read some more data
    $client->{_read_position} = 0;
    $poe_kernel->select_read( $client->{socket}, 'read_body_chunk', $ID );

    # We need to wait until all body data is read before returning
    $poe_kernel->get_active_session->wait( "prepare_body_done_$ID" );
}

sub read_body_chunk {
    my ( $kernel, $self, $handle, $ID ) = @_[ KERNEL, OBJECT, ARG0, ARG2 ];

    my $client = $self->{clients}->{$ID};

    DEBUG && warn "[$ID] [$$] read_body_chunk\n";

    if ( my $buffer_ref = $client->{driver}->get( $handle ) ) {
        for my $buffer ( @{$buffer_ref} ) {
#            DEBUG && warn "$buffer\n";
            $client->{_read_position} += length $buffer;
            $client->{context}->prepare_body_chunk( $buffer );
        }
    }

    my $body = $client->{context}->request->{_body};

    # paranoia against wrong Content-Length header
    if ( $client->{_read_position} > $body->length ) {
        $kernel->select_read( $handle );
        Catalyst::Exception->throw(
            "Wrong Content-Length value: " . $body->length
        );
        $kernel->yield( "prepare_body_done_$ID" );
        $kernel->yield( "prepare_done_$ID" );
        return;
    }

    # We're done when HTTP::Body's status changes to done
    if ( $body->state eq 'done' ) {
        # All done reading
        $kernel->select_read( $handle );
        $kernel->yield( "prepare_body_done_$ID" );
        $kernel->yield( "prepare_done_$ID" );
        return;
    }
}

# Finalize handles the entire finalize stage
sub finalize {
    my ( $self, $c ) = @_;

    my $ID = $c->{_POE_ID};
    my $client = $self->{clients}->{$ID};

    $poe_kernel->yield( 'handle_finalize', 'finalize_uploads', $ID );

    if ( $#{ $c->error } >= 0 ) {
        $poe_kernel->yield( 'handle_finalize', 'finalize_error', $ID );
    }

    $poe_kernel->yield( 'handle_finalize', 'finalize_headers', $ID );

    $poe_kernel->yield( 'handle_finalize', 'finalize_body', $ID );
    
    $poe_kernel->get_active_session->wait( "finalize_done_$ID" );
    
    # clean up everything about this client
    delete $self->{clients}->{$ID};
    
    return $c->response->status;
}

sub handle_finalize {
    my ( $kernel, $self, $method, $ID ) = @_[ KERNEL, OBJECT, ARG0, ARG1 ];
    
    DEBUG && warn "[$ID] [$$] - $method\n";

    my $client = $self->{clients}->{$ID};

    # Set the response body to null when we're doing a HEAD request.
    # Must be done here so finalize_headers can still set the proper
    # Content-Length value
    if ( $method eq 'finalize_body' ) {
        if ( $client->{context}->request->method eq 'HEAD' ) {
            $client->{context}->response->body('');
        }
    }

    $client->{context}->$method();
}

sub finalize_headers {
    my ( $self, $c ) = @_;

    my $client = $self->{clients}->{ $c->{_POE_ID} };

    my $protocol = $c->request->protocol;
    my $status   = $c->response->status;
    my $message  = HTTP::Status::status_message($status);

    $client->{wheel}->put( "$protocol $status $message\015\012" );
    $c->response->headers->date( time );

    $c->response->header( Status => $c->response->status );
    
    # XXX: Keep-Alive support?
    $c->response->header( Connection => 'close' );

    $client->{wheel}->put( $c->response->headers->as_string("\015\012") );
    $client->{wheel}->put( "\015\012" );
}

sub write {
    my ( $self, $c, $buffer ) = @_;

    my $ID = $c->{_POE_ID};
    my $client = $self->{clients}->{$ID};

    $client->{_highmark_reached} = $client->{wheel}->put( $buffer );

    # keep track of the amount of data we've sent
    $client->{_written} ||= 0;
    my $cl = $client->{context}->response->content_length;
    if ( $cl ) {
        $client->{_written} += length $buffer;
        DEBUG && warn "[$ID] [$$] written: " . $client->{_written} . "\n";
    }

    # if the output buffer has reached the highmark, we have a
    # lot of outgoing data.  Don't return until it's been sent
    while ( $client && $client->{_highmark_reached} ) {
        $poe_kernel->run_one_timeslice();
    }

    # always return 1, we can't detect failures here
    return 1;
}

# client_flushed is called when all data is done being written to the browser
sub client_flushed {
    my ( $kernel, $self, $ID ) = @_[ KERNEL, OBJECT, ARG0 ];

    my $client = $self->{clients}->{$ID};

    # Are we done writing?
    my $cl = $client->{context}->response->content_length;
    if ( $cl && $client->{_written} >= $cl ) {
        DEBUG && warn "[$ID] [$$] client_flushed, written full content-length\n";
        $kernel->yield( "finalize_done_$ID" );
    }

    # if we get this event because of the highmark being reached
    # don't clean up but reset the highmark value to 0
    if ( $client->{_highmark_reached} ) {
        $client->{_highmark_reached} = 0;
        return;
    }

    # we may have not had a content-length...
    $kernel->yield( "finalize_done_$ID" );
}

1;

=head1 NAME

Catalyst::Engine::HTTP::POE - Single-threaded multi-tasking Catalyst engine

=head1 SYNOPIS

    CATALYST_ENGINE='HTTP::POE' script/yourapp_server.pl
    
    # Prefork 5 children
    CATALYST_POE_MAX_PROC=6 CATALYST_ENGINE='HTTP::POE' script/yourapp_server.pl

=head1 DESCRIPTION

This engine allows Catalyst to process multiple requests in parallel within a
single process.  Much of the internal Catalyst flow now uses POE yield calls.
Application code will still block of course, but all I/O, header processing, and
POST body processing is handled asynchronously.

A good example of the engine's power is the L<Catalyst::Plugin::UploadProgress> demo
application, which can process a file upload as well as an Ajax polling request
at the same time in the same process.

This engine requires at least Catalyst 5.67.

=head1 RESTART SUPPORT

As of version 0.05, the -r flag is supported and the server will restart itself when any
application files are modified.

=head1 PREFORKING

As of version 0.05, the engine is able to prefork a set number of child processes to distribute
requests.  Set the CATALYST_POE_MAX_PROC environment variable to the total number of processes
you would like to run, including the parent process.  So, to prefork 5 children, set this value
to 6.  This value may also be set by modifying yourapp_server.pl and adding max_proc to the
options hash passed to YourApp->run().

=head1 DEBUGGING

To enable trace-level debugging, set the environment variable CATALYST_POE_DEBUG.

At any time you can get a dump of the internal state of the engine by sending a
USR1 signal to the running process.

=head1 EXPERIMENTAL STATUS

This engine should still be considered experimental and likely has bugs, however as
it's only intended for development, please use it and report bugs.

The engine has been tested with the UploadProgress demo, the Streaming example,
and one of my own moderately large applications.  It also fully passes the Catalyst
test suite.

=head1 AUTHOR

Andy Grundman, <andy@hybridized.org>

=head1 COPYRIGHT

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut