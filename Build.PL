use strict;
use Module::Build;

my $build = Module::Build->new(
    create_makefile_pl => 'passthrough',
    license            => 'perl',
    module_name        => 'Catalyst::Engine::HTTP::POE',
    requires           => {
        'Catalyst' => '5.67',
        'POE'      => '0',
    },
    create_makefile_pl => 'passthrough',
    create_readme      => 1,
    test_files   => [
        glob('t/0*.t') 
    ],
    create_readme => 1,
);
$build->create_build_script;
