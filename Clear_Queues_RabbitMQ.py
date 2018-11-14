import argparse
import datetime
import re
import shlex
import subprocess

import pika


def run(cmd):
    return subprocess.check_output(shlex.split(cmd))


def get_stuck_queues():
    # output = read_file()
    output = run('/sbin/rabbitmqctl list_queues name consumers')
    queues = []
    _type = ['VEHICLE', 'PASSENG']
    for queue in output.splitlines():
        line = re.split(r'\t+', queue)
        if str(line[-1]) == '0' and str(line[0][:7]) in _type:
            queues.append(line[0])
    return queues


def delete_queues(_user, _pass, _ip_address, _port):
    credentials = pika.PlainCredentials(_user, _pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        _ip_address, _port, '/', credentials))

    channel = connection.channel()
    for queue in get_stuck_queues():
        print "Purging queue: %s" % queue
        channel.queue_purge(queue=queue)
        channel.queue_delete(queue=queue)
    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-u', help='User Name of rabbitmq', action='store', dest='_user',
                        default='guess')

    parser.add_argument('-p', help='password of rabbitmq', action='store', dest='_password',
                        default='guess')

    parser.add_argument('-d', help='IP of rabbitmq', action='store', dest='_ip_address',
                        default='localhost')

    parser.add_argument('--port', help='Port of rabbitmq', action='store', dest='_port',
                        default=5672, type=int)
    arg = parser.parse_args()

    print '#' * 60
    _datetime = datetime.datetime.today()
    print 'Start running crontab at %s' % _datetime
    print '#' * 60
    try:
        delete_queues(arg._user, arg._password, arg._ip_address, arg._port)
    except Exception as ex:
        print (ex)
