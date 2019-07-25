// Copyright 2017 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package conductor

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/flaviostutz/conductor-go-client/task"
)

var (
	hostname, hostnameError = os.Hostname()
)

func init() {
	if hostnameError != nil {
		log.Fatal("Could not get hostname")
	}
}

type ConductorWorker struct {
	ConductorHttpClient      *ConductorHttpClient
	ThreadCount              int
	PollingInterval          int
	LongPollingTimeoutMillis int
}

func NewConductorWorker(baseUrl string, threadCount int, pollingInterval int, longPollingTimeoutMillis int) *ConductorWorker {
	conductorWorker := new(ConductorWorker)
	conductorWorker.ThreadCount = threadCount
	conductorWorker.PollingInterval = pollingInterval
	conductorWorker.LongPollingTimeoutMillis = longPollingTimeoutMillis
	conductorHttpClient := NewConductorHttpClient(baseUrl)
	conductorWorker.ConductorHttpClient = conductorHttpClient
	return conductorWorker
}

func (c *ConductorWorker) Execute(t *task.Task, executeFunction func(t *task.Task) (*task.TaskResult, error)) {
	taskResult, err := executeFunction(t)
	if err != nil {
		if taskResult == nil {
			taskResult = task.NewTaskResult(t)
		}
		log.Println("Error Executing task:", err.Error())
		taskResult.Status = task.FAILED
		taskResult.ReasonForIncompletion = err.Error()
	}

	if taskResult == nil {
		log.Println(fmt.Sprintf("'taskResult' cannot be nil on task execution return. taskType=%s", t.TaskType))
		return
	}

	taskResultJsonString, err := taskResult.ToJSONString()
	if err != nil {
		log.Println(err.Error())
		log.Println("Error Forming TaskResult JSON body")
		return
	}
	c.ConductorHttpClient.UpdateTask(taskResultJsonString)
}

func (c *ConductorWorker) PollAndExecute(taskType string, executeFunction func(t *task.Task) (*task.TaskResult, error)) {
	for {
		time.Sleep(time.Duration(c.PollingInterval) * time.Millisecond)

		// Poll for Task taskType
		polled, err := c.ConductorHttpClient.PollForTaskTimeout(taskType, hostname, c.LongPollingTimeoutMillis)
		if err != nil {
			log.Println("Error Polling task:", err.Error())
			continue
		}
		if polled == "" || polled == "[]" {
			log.Println("No tasks found for:", taskType)
			continue
		}

		ts := make([]task.Task, 0)
		err1 := json.Unmarshal([]byte(polled), &ts)
		if err1 != nil {
			log.Println("Error parsing result array:", err1)
			continue
		}

		for _, parsedTask := range ts {
			parsedTask.CallbackFromWorker = true

			// Found a task, so we send an Ack
			_, ackErr := c.ConductorHttpClient.AckTask(parsedTask.TaskId, hostname)
			if ackErr != nil {
				log.Println("Error Acking task:", ackErr.Error())
				continue
			}

			// Execute given function
			c.Execute(&parsedTask, executeFunction)
		}
	}
}

func (c *ConductorWorker) Start(taskType string, executeFunction func(t *task.Task) (*task.TaskResult, error), wait bool) {
	log.Println("Polling for task:", taskType, "with a:", c.PollingInterval, "(ms) polling interval with", c.ThreadCount, "goroutines for task execution, with workerid as", hostname)
	for i := 1; i <= c.ThreadCount; i++ {
		go c.PollAndExecute(taskType, executeFunction)
	}

	// wait infinitely while the go routines are running
	if wait {
		select {}
	}
}
