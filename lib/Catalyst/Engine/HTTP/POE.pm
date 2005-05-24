package Catalyst::Engine::HTTP::POE;

use strict;
use base 'Catalyst::Engine::HTTP::Base';

use POE::Component::Server::HTTP;

our $VERSION = '0.01';

=head1 NAME

Catalyst::Engine::HTTP::POE - Catalyst HTTP POE Engine

=head1 SYNOPSIS

A script using the Catalyst::Engine::HTTP::POE module might look like:

    #!/usr/bin/perl -w

    BEGIN {  $ENV{CATALYST_ENGINE} = 'HTTP::POE' }

    use strict;
    use lib '/path/to/MyApp/lib';
    use MyApp;

    MyApp->run;

=head1 DESCRIPTION

This the Catalyst engine use C<POE::Component::Server::HTTP>.

=head1 OVERLOADED METHODS

This class overloads some methods from C<Catalyst::Engine::HTTP::Base>.

=over 4

=item $c->run

=cut

sub run {
    my $class = shift;
    my $port  = shift || 3000;

    my $httpd = POE::Component::Server::HTTP->new(
        ContentHandler => {
            '/' => sub { $class->handler(@_) }
        },
        Headers => {
            Server => "Catalyst/$Catalyst::VERSION"
        },
        Port    => $port
    );

    POE::Kernel->run;
}

=item $c->handler

=cut

sub handler {
    my ( $class, $request, $response ) = @_;

    $response->protocol( $response->protocol );

    my $http = Catalyst::Engine::HTTP::Base::struct->new(
        address  => $request->connection->remote_ip,
        hostname => $request->connection->remote_host,
        request  => $request,
        response => $response
    );

    $class->SUPER::handler($http);
    
    return RC_OK;
}

=back

=head1 SEE ALSO

L<Catalyst>, L<Catalyst::Engine>, L<Catalyst::Engine::HTTP::Base>,
L<POE::Component::Server::HTTP>.

=head1 AUTHOR

Christian Hansen, C<ch@ngmedia.com>

=head1 COPYRIGHT

This program is free software, you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut

1;

