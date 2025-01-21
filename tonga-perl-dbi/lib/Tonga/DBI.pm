package Tonga::DBI;

use strict;
use warnings;
use Moo;

has dbh => (
    is       => 'ro',
    isa      => 'DBI::db',
    required => 1,
);

sub create_channel {
    my ($self, $queue_name, %opts) = @_;
    my $sth = $dbh->prepare('select * from tonga_create_channel(queue_name => ?, delete_after => ?, unlogged => ?);');
    $sth->execute($queue_name, $opts{delete_after}, $opts{unlogged} ? 1 : 0);
}

sub delete_channel {
    my ($self, $queue_name) = @_;
    my $sth = $dbh->prepare('select * from tonga_delete_channel(queue_name => ?);');
    $sth->execute($queue_name);
    my ($existed) = $sth->fetchrow_array();
    return $existed;
}

sub send {
    my ($self, $topic, $body, %opts) = @_;
    my $sth = $dbh->prepare('select * from tonga_send(topic => ?, body => ?, deliver_at => ?);');
    $sth->execute($topic, $body, $opts{deliver_at});
}

sub read {
    my ($self, $queue_name, $hide_for, $quantity) = @_;
    my $sth = $dbh->prepare('select * from tonga_read(queue_name => ?, hide_for => ?, quantity => ?);');
    $sth->execute($queue_name, $hide_for, $quantity);
    return $sth->fetchall_arrayref({});
}

sub delete {
    my ($self, $queue_name, $id) = @_;
    my $sth = $dbh->prepare('select * from tonga_delete(queue_name => ?, id => ?);');
    $sth->execute($queue_name, $id);
    my ($existed) = $sth->fetchrow_array();
    return $existed;
}

sub gc {
    my ($self) = @_;
    my $sth = $dbh->prepare('select * from tonga_gc();');
    $sth->execute();
}

1;