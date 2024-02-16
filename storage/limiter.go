// Copyright JAMF Software, LLC

package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jamf/regatta/storage/kv"
)

type burst struct {
	Tokens   uint64        `json:"a"`
	Interval time.Duration `json:"i"`
	ValidTo  time.Time     `json:"v"`
}

type state struct {
	Tokens    uint64        `json:"a"`
	Interval  time.Duration `json:"i"`
	Remaining uint64        `json:"r"`
	LastTake  time.Time     `json:"l"`
	Burst     burst         `json:"b,omitempty"`
	Ver       uint64        `json:"-"`
}

type KVLimiter struct {
	rs    *kv.RaftStore
	clock clock.Clock
	mtx   sync.Mutex
}

func (k *KVLimiter) prefixKey(key string) string {
	return fmt.Sprintf("ratelimit/%s", key)
}

func (k *KVLimiter) getState(key string) (state, error) {
	get, err := k.rs.Get(k.prefixKey(key))
	s := state{}
	if err != nil {
		if errors.Is(err, kv.ErrNotExist) {
			return s, nil
		}
		return s, err
	}
	if err := json.Unmarshal([]byte(get.Value), &s); err != nil {
		return s, err
	}
	s.Ver = get.Ver
	return s, nil
}

func (k *KVLimiter) setState(key string, new state, version uint64) (state, error) {
	b, err := json.Marshal(new)
	if err != nil {
		return new, err
	}
	p, err := k.rs.Set(k.prefixKey(key), string(b), version)
	if err != nil {
		return new, err
	}
	new.Ver = p.Ver
	return new, nil
}

func (k *KVLimiter) Take(key string) (remaining uint64, reset time.Time, ok bool, err error) {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	s, err := k.getState(key)
	if err != nil {
		return 0, k.clock.Now(), false, err
	}

	now := k.clock.Now()
	next := now

	if s.Burst.ValidTo.Before(now) {
		s.Burst = burst{}
	}

	if !s.LastTake.IsZero() {
		fromLast := now.Sub(s.LastTake)
		if s.Burst.ValidTo.After(now) {
			next = s.LastTake.Add(s.Burst.Interval)
			nums := fromLast / s.Burst.Interval
			if nums > 1 {
				s.Remaining += s.Burst.Tokens * uint64(nums)
			}
		} else {
			next = s.LastTake.Add(s.Interval)
			nums := fromLast / s.Interval
			if nums > 1 {
				s.Remaining += s.Tokens * uint64(nums)
			}
		}
	}

	if s.Remaining > 0 {
		s.Remaining--
		s.LastTake = now
		news, err := k.setState(key, s, s.Ver)
		if err != nil {
			return 0, next, false, err
		}
		return news.Remaining, next, true, nil
	}
	return s.Remaining, next, false, nil
}

func (k *KVLimiter) Get(key string) (tokens, remaining uint64, burst bool, err error) {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	s, err := k.getState(key)
	if err != nil {
		return 0, 0, false, err
	}
	return s.Tokens, s.Remaining, s.Burst.ValidTo.After(k.clock.Now()), nil
}

func (k *KVLimiter) Reset(key string, tokens uint64, interval time.Duration) error {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	cur, err := k.getState(key)
	if err != nil {
		return err
	}
	_, err = k.setState(key, state{Tokens: tokens, Remaining: tokens, Interval: interval, LastTake: time.Time{}}, cur.Ver)
	return err
}

func (k *KVLimiter) Burst(key string, tokens uint64, interval, validity time.Duration) error {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	s, err := k.getState(key)
	if err != nil {
		return err
	}
	s.Burst = burst{
		Tokens:   tokens,
		Interval: interval,
		ValidTo:  k.clock.Now().Add(validity),
	}
	_, err = k.setState(key, s, s.Ver)
	return err
}
