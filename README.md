# 🤔 Glossim Glomers Distributed Systems Challenge hosted by Fly.io

This challenge composed of 5 parts, each part is a distributed system challenge.

## Part 1: Echo [maelstrom-echo](./maelstrom-echo)

- This part is a simple echo service, it will echo the message back to the sender.
- This is where we introduced to the `maelstrom` library, it is a library that helps us to build distributed systems.

## Part 2: Unique IDs [maelstrom-unique-ids](./maelstrom-unique-ids)

- This part is a unique ID generator, it will generate a globally-unique ID for each request.
- This part I just used `UUID` to generate the unique ID.

## Part 3: Broadcast 
- This is where the real challenges begin 🔥

### Part 3.1: Broadcast [maelstrom-single-node-broadcast](./maelstrom-single-node-broadcast)

- This one is a single node broadcast service, but just a single node broadcast systems. 
- This one is pretty straight forward approach, I used the array to stored all the messages and when there is a `read` request, I just send that array back.

### Part 3.2: Broadcast [maelstrom-multi-node-broadcast](./maelstrom-multi-node-broadcast)

- This one is a multi-node broadcast service, it will broadcast the message to all the nodes in the cluster.
- I stored the neighbors from a `topology` request and broadcast to the neighbors using goroutines with best effort delivery.
- logic is pretty simple, I stored the `map of message` and if the message wasn't recognized before I will just send to my neighbors.

### Part 3.3: Fault Tolerance [maelstrom-multi-node-fault-tolerance](./maelstrom-multi-node-fault-tolerance)
- I use goroutine to wakes up every second to check on message delivery status.
- New message get 3 attempst with 5ms cooldown
- Any unsuccessful message move to retry queue then they get 5 more attempts with 5ms delay

### Part 3.4 and 3.5: Efficient Broadcast [maelstrom-efficient-broadcast](./maelstrom-efficient-broadcast)
- I think I will skip this because of my skill issues when I'm better I will come back to this.

### Part 4: Grow-Only Counter [maelstrom-grow-only-counter](./maelstrom-grow-only-counter)
- I used sequence key-value store that is provided by maelstrom lib to implement this part think of something like this.
```
[Node 1] ----\
[Node 2] ----- [seq-kv service] (stores the counter provided by maelstrom)
[Node 3] ----/
```

### Part 5.1: Kafka-style Log [maelstrom-kafka-style-log-v2](./maelstrom-kafka-style-log-v2)
- This part is a kafka-style log service, it will store the log and provide a way to poll the log.
