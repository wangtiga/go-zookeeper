package zk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked   = errors.New("zk: not locked")
	ErrLockTimeOut = errors.New("zk: lock timeout")
	ErrLockBusy    = errors.New("zk: lock busy, too many queue")
)

// Lock is a mutual exclusion lock.
type Lock struct {
	c        *Conn
	path     string
	acl      []ACL
	lockPath string
	seq      int
}

// NewLock creates a new lock instance using the provided connection, path, and acl.
// The path must be a node that is only used by this lock. A lock instances starts
// unlocked until Lock() is called.
func NewLock(c *Conn, path string, acl []ACL) *Lock {
	return &Lock{
		c:    c,
		path: path,
		acl:  acl,
	}
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

func RecursionCreatePath(c *Conn, path string, acl []ACL) error {
	// Create parent node.
	parts := strings.Split(path, "/")
	pth := ""
	for _, p := range parts[1:] {
		var exists bool
		pth += "/" + p
		exists, _, err := c.Exists(pth)
		if err != nil {
			return err
		}
		if exists == true {
			continue
		}
		if _, err = c.Create(pth, []byte{}, 0, acl); err != nil && err != ErrNodeExists {
			return err
		}
	}
	return nil
}

// Lock attempts to acquire the lock. It will wait to return until the lock
// is acquired or an error occurs. If this instance already has the lock
// then ErrDeadlock is returned.
func (l *Lock) Lock() error {
	return l.TryLock(-1, time.Duration(24)*time.Hour)
}

func (l *Lock) TryLock(maxQueue int, maxTimeOut time.Duration) error {
	if l.lockPath != "" {
		return ErrDeadlock
	}
	if maxQueue > 0 {
		children, _, err := l.c.Children(l.path)
		if nil == err {
			// 忽略错误，因为第一次加锁，肯定会出现 ErrNoNode
			if len(children) > maxQueue {
				return ErrLockBusy
			}
		}
	}

	prefix := fmt.Sprintf("%s/lock-", l.path)

	lockPath := ""
	var err error
	for i := 0; i < 3; i++ {
		lockPath, err = l.c.CreateProtectedEphemeralSequential(prefix, []byte{}, l.acl)
		if err == ErrNoNode {
			if err := RecursionCreatePath(l.c, l.path, l.acl); nil != err {
				return err
			}
		} else if err == nil {
			break
		} else {
			return err
		}
	}
	if err != nil {
		return err
	}

	seq, err := parseSeq(lockPath)
	if err != nil {
		return err
	}

	for {
		children, _, err := l.c.Children(l.path)
		if err != nil {
			return err
		}

		lowestSeq := seq
		prevSeq := -1
		prevSeqPath := ""
		for _, p := range children {
			s, err := parseSeq(p)
			if err != nil {
				return err
			}
			if s < lowestSeq {
				lowestSeq = s
			}
			if s < seq && s > prevSeq {
				prevSeq = s
				prevSeqPath = p
			}
		}

		if seq == lowestSeq {
			// Acquired the lock
			break
		}

		// Wait on the node next in line for the lock
		_, _, ch, err := l.c.GetW(l.path + "/" + prevSeqPath)
		if err != nil && err != ErrNoNode {
			return err
		} else if err != nil && err == ErrNoNode {
			// try again
			continue
		}

		select {
		case ev := <-ch:
			{
				if ev.Err != nil {
					return ev.Err
				}
			}
		case <-time.After(maxTimeOut):
			{
				if err := l.c.Delete(lockPath, -1); err != nil {
					return err
				}
				if err := l.c.Delete(l.path, -1); err != nil && err != ErrNotEmpty {
					fmt.Println("Unlock error del path=", l.path, ",err", err)
				}
				return ErrLockTimeOut
			}
		}
	}

	l.seq = seq
	l.lockPath = lockPath
	return nil
}

// Unlock releases an acquired lock. If the lock is not currently acquired by
// this Lock instance than ErrNotLocked is returned.
func (l *Lock) Unlock() error {
	if l.lockPath == "" {
		return ErrNotLocked
	}
	if err := l.c.Delete(l.lockPath, -1); err != nil {
		return err
	}
	if err := l.c.Delete(l.path, -1); err != nil && err != ErrNotEmpty {
		fmt.Println("Unlock error del path=", l.path, ",err", err)
	}
	l.lockPath = ""
	l.seq = 0
	return nil
}
