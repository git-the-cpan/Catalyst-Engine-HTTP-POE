use strict;
use Module::Build;

my $build = Module::Build->new(
    create_makefile_pl => 'passthrough',
    license            => 'perl',
    module_name        => 'Catalyst::Engine::HTTP::POE',
    requires           => {
        'Catalyst'                     => 5.2,
        'POE'                          => 0,
        'POE::Component::Server::HTTP' => 0
    },
    create_makefile_pl => 'passthrough',
    test_files   => [
        glob('t/*.t') 
    ]
);
$build->create_build_script;
