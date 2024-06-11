import threading
import time
from queue import Queue, Empty

class MessageManager:

    def __init__(self, num_recipients):
        self.local_clock = 0
        self.message_queue = []
        self.commit_queues = {recipient: Queue() for recipient in range(num_recipients)}
        self.should_stop = False
        self.message_order = []

    def send_unordered_message(self, content, recipient):
        print(f"Sending an unordered message: '{content}' to recipient: {recipient}")

    def send_total_order_message(self, content, recipients):
        self.local_clock += 1
        message = (content, self.local_clock, recipients)
        self.message_queue.append(message)
        self.broadcast_ack(message)

    def broadcast_ack(self, message):
        acks = {}
        for recipient in message[2]:
            ack = self.receive_ack(recipient)
            acks[recipient] = ack

        if all(ack is not None for ack in acks.values()):
            max_time = max(ack[1] for ack in acks.values())
            for recipient in message[2]:
                self.commit_queues[recipient].put((message[0], max_time))

    def receive_ack(self, recipient):
        time.sleep(0.1)  # Simulate network delay
        ack_time = self.local_clock
        return (recipient, ack_time)

    def commit_messages(self, recipient):
        commit_queue = self.commit_queues[recipient]
        while not self.should_stop:
            try:
                message, max_time = commit_queue.get(timeout=1)
            except Empty:
                continue

            if self.local_clock < max_time:
                self.local_clock = max_time
            self.local_clock += 1
            self.message_order.append((message, self.local_clock))
            print(f"Committing message: '{message}' at time {self.local_clock}")

    def stop_commit_threads(self):
        self.should_stop = True
        self.message_order.sort(key=lambda x: x[1])
        for message, _ in self.message_order:
            print(f"Total order message received: '{message}'")

    def print_total_order_messages(self):
        while not self.should_stop:
            if len(self.message_order) > 0:
                message, _ = self.message_order.pop(0)
                print(f"Total order message received: '{message}'")

if __name__ == "__main__":
    num_recipients = int(input("Enter the number of recipients: "))
    num_total_order_messages = int(input("Enter the number of total order messages to send: "))

    manager = MessageManager(num_recipients)

    for i in range(num_recipients):
        manager.send_unordered_message(f"Unordered Message {i}", i % num_recipients)

    threads = []

    for i in range(num_recipients):
        t = threading.Thread(target=manager.commit_messages, args=(i,))
        threads.append(t)
        t.start()

    for i in range(num_total_order_messages):
        recipients = [j % num_recipients for j in range(num_recipients)]
        manager.send_total_order_message(f"Message {i}", recipients)

    print_thread = threading.Thread(target=manager.print_total_order_messages)
    print_thread.start()

    manager.stop_commit_threads()

    for t in threads:
        t.join()

    print_thread.join()
