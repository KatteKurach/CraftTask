# -*- coding: utf-8 -*-

import csv
import urllib.robotparser as pr
import threading
import Queue


class Task(object):
    def __init__(self, func, args):
        self.func = func
        self.args = args[:]
        

    def run(self):
        self.func(*self.args)


def addPoisonPill(queue, threadCount):
    for _ in range(threadCount):
        queue.put(None)


def processUrl(url, writer, writerLock):
    pass

def readQueueFromFile(filename, queue):
    with open(filename, 'r') as table:
        reader = csv.reader(table)
        for line in reader:
            queue.put(Task(processUrl, [line[0]])


def writeLine(writer, line):
    pass


def getCrawlDelay(url):
    pass

def threadLoop(queue, condition, writer, writerLock):
    pass

def delayLoop(delayCondition, delay, threads):
    pass


def main():
    inputFile = 'wikipedia_links.csv' 
    outputFile = 'temp.csv'
    threadCount = 4

    delayCondition = threading.Condition(threading.Lock())
    writerLock = threading.Lock()
    queue = Queue.Queue()
   
    threads = []
    f = open(output)
    writer = csv.writer(f, delimeter = ',')

    for _ in range(threadCount):
        thread = threading.Thread(target = threadLoop, args = (queue, delayCondition, writer, writerLock))
        thread.start()
        threads.append(thread)
    delayManager = threading.Thread(target = delayLoop, args = (delayCondition, delay, threads))
    delayManager.start()

    getQueueFromFile(inputFile, queue)
    delayManager.join()
    f.close()

if __name__ == '__main__':
    main()
