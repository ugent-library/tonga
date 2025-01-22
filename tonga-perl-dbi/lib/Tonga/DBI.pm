package Tonga::DBI;

use strict;
use warnings;
use JSON::MaybeXS qw(encode_json decode_json);
use Types::Standard qw(InstanceOf);
use Moo;
use namespace::clean;

has dbh => (
    is       => 'ro',
    isa      => InstanceOf['DBI::db'],
    required => 1,
);

sub create_channel {
    my ($self, $queue_name, $topic, %opts) = @_;
    my $sth = $self->dbh->prepare('select * from tonga_create_channel(queue_name => ?, topic => ?, delete_at => ?, unlogged => ?);');
    $sth->execute($queue_name, $topic, $opts{delete_at}, $opts{unlogged} ? 1 : 0);
}

sub delete_channel {
    my ($self, $queue_name) = @_;
    my $sth = $self->dbh->prepare('select * from tonga_delete_channel(queue_name => ?);');
    $sth->execute($queue_name);
    my ($existed) = $sth->fetchrow_array();
    return $existed;
}

sub send {
    my ($self, $topic, $body, %opts) = @_;
    my $sth = $self->dbh->prepare('select * from tonga_send(topic => ?, body => ?, deliver_at => ?);');
    $sth->execute($topic, encode_json($body), $opts{deliver_at});
}

sub read {
    my ($self, $queue_name, $quantity, $hide_for) = @_;
    my $sth = $self->dbh->prepare('select * from tonga_read(queue_name => ?, quantity => ?, hide_for => ?);');
    $sth->execute($queue_name, $quantity, $hide_for);
    my $msgs = $sth->fetchall_arrayref({});
    for my $msg (@$msgs) {
        $msg->{body} = decode_json($msg->{body});
    }
    return $msgs;
}

sub delete {
    my ($self, $queue_name, $id) = @_;
    my $sth = $self->dbh->prepare('select * from tonga_delete(queue_name => ?, id => ?);');
    $sth->execute($queue_name, $id);
    my ($existed) = $sth->fetchrow_array();
    return $existed;
}

sub gc {
    my ($self) = @_;
    my $sth = $self->dbh->prepare('select * from tonga_gc();');
    $sth->execute();
}

1;