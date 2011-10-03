package Net::ZooKeeper::Lock;

use strict;
use warnings;

use Net::ZooKeeper qw(:events :node_flags :acls);
use Params::Validate qw(:all);

# ABSTRACT: distributed locks via ZooKeeper

=head1 SYNOPSIS

  use Net::ZooKeeper::Lock;

  # take a lock
  my $lock = Net::ZooKeeper::Lock->new({
      zkh => Net::ZooKeeper->new(host => 'localhost:2181'),
      lock_name   => 'bar',
  });

  # release a lock
  $lock->unlock;
  # or
  undef $lock;

=head1 DESCRIPTION

=head1 METHODS

=cut

sub new {
    my $class = shift;
    my $p = validate(@_, {
        zkh           => { isa => 'Net::ZooKeeper' },
        lock_prefix   => { type => SCALAR, regex => qr{^/.+}o, default => '/lock' },
        lock_name     => { type => SCALAR, regex => qr{^[^/]+$}o },
        create_prefix => { type => BOOLEAN, default => 1 },
        watch_timeout => { type => SCALAR, regex => qr/^\d+$/o, default => 86400 * 1000 },
        nonblocking   => { type => BOOLEAN, default => 0 },
    });
    $p->{lock_prefix} =~ s{/$}{};

    my $self = $p;
    bless $self, $class;

    $self->_lock;

    return $self;
}

sub unlock {
    my $self = shift;
    $self->{zkh}->delete($self->{lock_path}) if ($self->{lock_path});
}

sub _create_cyclic_path {
    my ($self, $path) = @_;

    my $current_index = 1;
    while ($current_index > 0) {
        $current_index = index($path, "/", $current_index + 1);
        my $current_path;
        if ($current_index > 0) {
            $current_path = substr($path, 0, $current_index);
        } else {
            $current_path = $path;
        }

        if (!$self->{zkh}->exists($path)) {
            $self->{zkh}->create($path, '0',
                acl => ZOO_OPEN_ACL_UNSAFE
            );
        }
    }
}
sub _lock {
    my $self = shift;

    my $zkh = $self->{zkh};
    my $lock_prefix = $self->{lock_prefix};
    my $lock_name = $self->{lock_name};

    if ($self->{create_prefix}) {
        $self->_create_cyclic_path($lock_prefix);
    }

    my $lock_tmpl = $lock_prefix . "/" . $lock_name . "-";
    my $lock_path = $zkh->create($lock_tmpl, '0',
        flags => (ZOO_EPHEMERAL | ZOO_SEQUENCE),
        acl   => ZOO_OPEN_ACL_UNSAFE) or
    die "unable to create sequence znode $lock_tmpl: " . $zkh->get_error . "\n";

    while (1) {
        my @child_names = $zkh->get_children($lock_prefix);
        die "no childs\n" unless (scalar(@child_names));

        my @less_than_me = sort
                           grep { ($_ =~ m/^${lock_name}-\d+$/) &&
                                  ($lock_prefix . "/" . $_ lt $lock_path) }
                           @child_names;

        unless (@less_than_me) {
            $self->{lock_path} = $lock_path;
            return;
        }

        if ($self->{nonblocking}) {
            die "lock already taken\n";
        }

        $self->_exists($lock_prefix . "/" . $less_than_me[-1]);
    }
}

sub _exists {
    my ($self, $path) = @_;

    my $zkh = $self->{zkh};

    my $watcher = $zkh->watch(timeout => $self->{watch_timeout});
    while (1) {
        my $exists = $zkh->exists(
            $path,
            watch => $watcher,
        );

        if (!$exists) {
            next;
        } else {
            last;
        }
    }
}

sub DESTROY {
    local $@;

    my $self = shift;

    $self->unlock;
};

1;
