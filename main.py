# -*- coding: utf-8 -*-

import csv
import threading
import queue as Queue
import time
import requests
from bs4 import BeautifulSoup as bf
import argparse


class Task(object):
    def __init__(self, func, args):
        self.func = func
        self.args = args[:]
        

    def run(self):
        self.func(*self.args)


def addPoisonPill(queue, threadCount):
    for _ in range(threadCount):
        queue.put(None)


def downloadPage(url):
    page = requests.get(url) 
    if  page.status_code != 200:
        raise Exception(page.status_code)
    return page.text


def processUrl(url, writer, writerLock):
    result = ''
    try:
        html = downloadPage(url)
        soup = bf(html, 'html.parser')
        table = soup.find('table', attrs={'class': 'infobox'})
        rows = table.find_all('tr')
        
        for row in rows:
            columnHeader = row.find_all('th')
            if 'Website' in str(columnHeader):
                a = row.find_all('a', attrs={'class': 'external'})
                result = a[0]['href']
                break
    finally:
        writeLine(writer, [url, result], writerLock)
    

def readQueueFromFile(filename, queue):
    with open(filename, 'r') as table:
        reader = csv.reader(table)
        for line in reader:
            queue.put(Task(processUrl, [line[0]]))


def writeLine(writer, line, writerLock):
    writerLock.acquire()
    writer.writerow(line)
    writerLock.release()


def getDelay(url):
    return 0.01 


def threadLoop(queue, delayCondition, writer, writerLock):
    while True:
        task = queue.get()
        if task is None:
            break

        task.args.append(writer)
        task.args.append(writerLock)
        delayCondition.acquire()
        delayCondition.wait()
        task.run()
        delayCondition.release()
        
        
def allPoisoned(threads):
    for thread in threads:
        if thread.is_alive():
            return False
    return True


def delayLoop(delayCondition, delay, threads):
    while True:
        if allPoisoned(threads):
            break
        delayCondition.acquire()
        delayCondition.notify()
        delayCondition.release()
        time.sleep(delay)
        

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('inputFile', help='Write name of input file.', type=str)
    args = parser.parse_args()

    inputFile = args.inputFile
    outputFile = 'wikipedia_answers.csv'
    threadCount = 4

    delayCondition = threading.Condition(threading.Lock())
    writerLock = threading.Lock()
    queue = Queue.Queue()
   
    threads = []
    f = open(outputFile, 'w')
    writer = csv.writer(f, delimiter = ',')
    writer.writerow(['wikipedia_page','website'])
    for _ in range(threadCount):
        thread = threading.Thread(target = threadLoop, args = (queue, delayCondition, writer, writerLock))
        thread.start()
        threads.append(thread)
    delayManager = threading.Thread(target = delayLoop, args = (delayCondition, getDelay('vf '), threads))
    delayManager.start()

    readQueueFromFile(inputFile, queue)
    addPoisonPill(queue, threadCount)
    delayManager.join()
    f.close()

if __name__ == '__main__':
    main()
