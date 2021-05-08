import pulsar
import time
client = pulsar.Client('pulsar://host.docker.internal:6650')

producer = client.create_producer('my-topic')
ctr = 0

while 1:
    producer.send(('Counter-%d' % ctr).encode('utf-8'))
    ctr += 1
    time.sleep(1)


client.close()
