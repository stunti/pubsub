// Copyright 2013, Chandra Sekar S.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the README.md file.

package pubsub

import (
	check "gopkg.in/check.v1"
	"runtime"
	"testing"
	"time"
)

var _ = check.Suite(new(Suite))

func Test(t *testing.T) {
	check.TestingT(t)
}

type Suite struct{}

func (s *Suite) TestSub(c *check.C) {
	ps := New(1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t1")
	ch3 := ps.Sub("t2")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}

	ps.Pub(msg1, "t1")
	res1 := <-ch1
	res2 := <-ch2

	c.Check(res1.Payload, check.Equals, "hi")
	c.Check(res2.Payload, check.Equals, "hi")

	msg3 := &Message{
		Id:      3,
		Payload: "hello",
	}
	ps.Pub(msg3, "t2")
	res3 := <-ch3
	c.Check(res3.Payload, check.Equals, "hello")

	ps.Shutdown()
	_, ok := <-ch1
	c.Check(ok, check.Equals, false)
	_, ok = <-ch2
	c.Check(ok, check.Equals, false)
	_, ok = <-ch3
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestSubOnce(c *check.C) {
	ps := New(1)
	ch := ps.SubOnce("t1")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")
	res := <-ch
	c.Check(res.Payload, check.Equals, "hi")

	_, ok := <-ch
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}

func (s *Suite) TestAddSub(c *check.C) {
	ps := New(1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	msg1 := &Message{
		Id:      1,
		Payload: "hi1",
	}
	ps.Pub(msg1, "t1")
	res1 := <-ch1
	c.Check(res1.Payload, check.Equals, "hi1")

	msg2 := &Message{
		Id:      2,
		Payload: "hi2",
	}
	ps.Pub(msg2, "t2")
	res2 := <-ch2
	c.Check(res2.Payload, check.Equals, "hi2")

	msg3 := &Message{
		Id:      3,
		Payload: "hi3",
	}
	ps.AddSub(ch1, "t2", "t3")
	ps.Pub(msg3, "t2")
	res1 = <-ch1
	res2 = <-ch2
	c.Check(res1.Payload, check.Equals, "hi3")
	c.Check(res2.Payload, check.Equals, "hi3")

	msg4 := &Message{
		Id:      4,
		Payload: "hi4",
	}
	ps.Pub(msg4, "t3")
	res1 = <-ch1
	c.Check(res1.Payload, check.Equals, "hi4")

	ps.Shutdown()
}

func (s *Suite) TestUnsub(c *check.C) {
	ps := New(1)
	ch := ps.Sub("t1")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")
	res := <-ch
	c.Check(res.Payload, check.Equals, "hi")

	ps.Unsub(ch, "t1")
	_, ok := <-ch
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}

func (s *Suite) TestUnsubAll(c *check.C) {
	ps := New(1)
	ch1 := ps.Sub("t1", "t2", "t3")
	ch2 := ps.Sub("t1", "t3")

	ps.Unsub(ch1)

	m, ok := <-ch1
	c.Check(ok, check.Equals, false)

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")
	m, ok = <-ch2
	c.Check(m.Payload, check.Equals, "hi")

	ps.Shutdown()
}

func (s *Suite) TestClose(c *check.C) {
	ps := New(1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t1")
	ch3 := ps.Sub("t2")
	ch4 := ps.Sub("t3")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	msg2 := &Message{
		Id:      2,
		Payload: "hello",
	}
	ps.Pub(msg1, "t1")
	ps.Pub(msg2, "t2")
	res1 := <-ch1
	res2 := <-ch2
	res3 := <-ch3
	c.Check(res1.Payload, check.Equals, "hi")
	c.Check(res2.Payload, check.Equals, "hi")
	c.Check(res3.Payload, check.Equals, "hello")

	ps.Close("t1", "t2")
	_, ok := <-ch1
	c.Check(ok, check.Equals, false)
	_, ok = <-ch2
	c.Check(ok, check.Equals, false)
	_, ok = <-ch3
	c.Check(ok, check.Equals, false)

	msg3 := &Message{
		Id:      3,
		Payload: "welcome",
	}
	ps.Pub(msg3, "t3")
	res4 := <-ch4
	c.Check(res4.Payload, check.Equals, "welcome")

	ps.Shutdown()
}

func (s *Suite) TestUnsubAfterClose(c *check.C) {
	ps := New(1)
	ch := ps.Sub("t1")
	defer func() {
		ps.Unsub(ch, "t1")
		ps.Shutdown()
	}()

	ps.Close("t1")
	_, ok := <-ch
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestShutdown(c *check.C) {
	start := runtime.NumGoroutine()
	New(10).Shutdown()
	time.Sleep(1)
	c.Check(runtime.NumGoroutine()-start, check.Equals, 1)
}

func (s *Suite) TestMultiSub(c *check.C) {
	ps := New(1)
	ch := ps.Sub("t1", "t2")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")
	res := <-ch
	c.Check(res.Payload, check.Equals, "hi")

	msg2 := &Message{
		Id:      1,
		Payload: "hello",
	}
	ps.Pub(msg2, "t2")
	res = <-ch
	c.Check(res.Payload, check.Equals, "hello")

	ps.Shutdown()
	_, ok := <-ch
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestMultiSubOnce(c *check.C) {
	ps := New(1)
	ch := ps.SubOnce("t1", "t2")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")
	res := <-ch
	c.Check(res.Payload, check.Equals, "hi")

	msg2 := &Message{
		Id:      2,
		Payload: "hello",
	}
	ps.Pub(msg2, "t2")

	_, ok := <-ch
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}

func (s *Suite) TestMultiPub(c *check.C) {
	ps := New(1)
	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1", "t2")
	res1 := <-ch1
	res2 := <-ch2
	c.Check(res1.Payload, check.Equals, "hi")
	c.Check(res2.Payload, check.Equals, "hi")

	ps.Shutdown()
}

func (s *Suite) TestMultiUnsub(c *check.C) {
	ps := New(1)
	ch := ps.Sub("t1", "t2", "t3")

	ps.Unsub(ch, "t1")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")

	msg2 := &Message{
		Id:      2,
		Payload: "hello",
	}
	ps.Pub(msg2, "t2")
	res := <-ch
	c.Check(res.Payload, check.Equals, "hello")

	ps.Unsub(ch, "t2", "t3")
	_, ok := <-ch
	c.Check(ok, check.Equals, false)

	ps.Shutdown()
}

func (s *Suite) TestMultiClose(c *check.C) {
	ps := New(1)
	ch := ps.Sub("t1", "t2")

	msg1 := &Message{
		Id:      1,
		Payload: "hi",
	}
	ps.Pub(msg1, "t1")
	res := <-ch
	c.Check(res.Payload, check.Equals, "hi")

	ps.Close("t1")

	msg2 := &Message{
		Id:      2,
		Payload: "hello",
	}

	ps.Pub(msg2, "t2")
	res = <-ch
	c.Check(res.Payload, check.Equals, "hello")

	ps.Close("t2")
	_, ok := <-ch
	c.Check(ok, check.Equals, false)

	ps.Shutdown()
}
