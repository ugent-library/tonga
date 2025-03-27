package tonga

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Conn interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row
}

type Client struct {
	conn Conn
}

func New(conn Conn) *Client {
	return &Client{conn: conn}
}

type ChannelOpts struct {
	DeleteAt time.Time
	Unlogged bool
}

func (c *Client) CreateChannel(ctx context.Context, queueName, topic string, opts ChannelOpts) error {
	if opts.DeleteAt.IsZero() {
		q := `select * from tonga_create_channel(queue_name => $1, topic => $2, unlogged => $3);`
		_, err := c.conn.Exec(ctx, q, queueName, topic, opts.Unlogged)
		return err
	} else {
		q := `select * from tonga_create_channel(queue_name => $1, topic => $2, delete_at => $3, unlogged => $4);`
		_, err := c.conn.Exec(ctx, q, queueName, topic, opts.DeleteAt, opts.Unlogged)
		return err
	}
}

func (c *Client) DeleteChannel(ctx context.Context, queueName string) (bool, error) {
	q := `select * from tonga_delete_channel(queue_name => $1);`
	existed := false
	err := c.conn.QueryRow(ctx, q, queueName).Scan(&existed)
	return existed, err
}

type SendOpts struct {
	DeliverAt time.Time
}

func (c *Client) Send(ctx context.Context, topic string, body any, opts SendOpts) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		q := `select * from tonga_send(topic => $1, body => $2);`
		_, err := c.conn.Exec(ctx, q, topic, b)
		return err
	} else {
		q := `select * from tonga_send(topic => $1, body => $2, deliver_at => $3);`
		_, err := c.conn.Exec(ctx, q, topic, b, opts.DeliverAt)
		return err
	}
}

type Message struct {
	ID        int64
	Topic     string
	Body      json.RawMessage
	CreatedAt time.Time
	DeliverAt time.Time
}

func (c *Client) Read(ctx context.Context, queueName string, quantity int, hideFor time.Duration) ([]*Message, error) {
	q := `select * from tonga_read(queue_name => $1, quantity => $2, hide_for => $3);`
	rows, err := c.conn.Query(ctx, q, queueName, quantity, hideFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[Message])
}

func (c *Client) Delete(ctx context.Context, queueName string, id int64) (bool, error) {
	q := `select * from tonga_delete(queue_name => $1, id => $2);`
	existed := false
	err := c.conn.QueryRow(ctx, q, queueName, id).Scan(&existed)
	return existed, err
}

func (c *Client) GC(ctx context.Context) error {
	q := `select * from tonga_gc();`
	_, err := c.conn.Exec(ctx, q)
	return err
}
