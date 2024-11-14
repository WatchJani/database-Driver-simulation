package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

func main() {
	connectionManager := NewDriver(SetNumberOfConnection(5))
	reqHandler := NewReqSimulation(connectionManager)

	time.Sleep(time.Second)
	var wg sync.WaitGroup

	wg.Add(10)
	for range 10 {
		go reqHandler.Req(&wg)
	}

	wg.Wait()
}

type ReqSimulation struct {
	*Driver
}

func NewReqSimulation(driver *Driver) ReqSimulation {
	return ReqSimulation{
		Driver: driver,
	}
}

func (r *ReqSimulation) Req(wg *sync.WaitGroup) {
	res := r.CreateQuery()

	fmt.Println(<-res)

	wg.Done()
}

type CommunicatorMap struct {
	communication map[string]chan string
	sync.RWMutex
}

func NewCommunicatorMap() CommunicatorMap {
	return CommunicatorMap{
		communication: make(map[string]chan string),
	}
}

func (c *CommunicatorMap) Set(key string, value chan string) {
	c.Lock()
	defer c.Unlock()

	c.communication[key] = value
}

func (c *CommunicatorMap) Get(key string) (chan string, error) {
	if value, ok := c.communication[key]; ok {
		return value, nil
	}

	return nil, fmt.Errorf("this key [%s] not exist", key)
}

type OptFunc func(*Driver)

type Driver struct {
	numberOfConnection int
	CommunicatorMap
	create chan string
}

func SetNumberOfConnection(num int) func(*Driver) {
	return func(d *Driver) {
		d.numberOfConnection = num
	}
}

func NewDriver(fn ...OptFunc) *Driver {
	sr := &Driver{
		numberOfConnection: 3,
		CommunicatorMap:    NewCommunicatorMap(),
		create:             make(chan string),
	}

	for _, fn := range fn {
		fn(sr)
	}

	//simulate conn to database
	for index := range sr.numberOfConnection {
		go sr.Conn(index)
	}

	return sr
}

func (d *Driver) CreateQuery() <-chan string {
	resCh := make(chan string)
	key := generateKey(5)
	d.Set(key, resCh)
	d.create <- key

	return resCh
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateKey(length int) string {

	key := make([]byte, length)
	for i := range key {
		key[i] = charset[rand.Intn(len(charset))]
	}
	return string(key)
}

// this is listener, but real driver have sender to
func (d *Driver) Conn(index int) {
	for {
		key := <-d.create

		//connection latency simulation
		time.Sleep(200 * time.Millisecond)
		fmt.Printf("worker %d done job\n", index)

		communicator, err := d.Get(key)
		if err != nil {
			log.Println(err)
			continue
		}
		communicator <- fmt.Sprintf("%d", index)
	}
}
