from multiprocessing import Queue
from concurrent import futures
from selenium import webdriver
from bs4 import BeautifulSoup
import threading
import time
import json
import io
import os

class TinyFetcher:
	def load(self, url):
		driver = webdriver.Firefox()
		try:
			driver.get(url)
			return driver
		except:
			driver.quit()
			return self.load(url)

	def handleCallbacks(self, element, appender, handler):
		appender(element)
		handler(element)
	
	def fetchIndex(self, appender, handler):
		pass
	
	def fetchContent(self, index, appender, handler):
		pass

class TinySerializer:
	charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ "
	def readIndex(self):
		try:
			indexes = json.load(io.open('.index', 'a+', encoding='utf-8'))
			for index in indexes:
				self.appendIndex(index)
		except:
			self.index = []
		return self
	
	def recover(self, handler):
		for index in self.index:
			if not index["finished"]:
				handler(index)
		return self
	
	def saveIndex(self):
		json.dump(self.index, io.open('.index', 'w', encoding='utf-8'))
	
	def appendIndex(self, element):
		for elem in self.index:
			if elem["title"] == element["title"]:
				element["finished"] = True
				return
		self.index.append(element)

	def appendContent(self, element):
		title = element["title"]
		filename = ""
		for ch in title:
			if ch in TinySerializer.charset:
				filename += ch
		path = os.path.join("contents", filename + ".txt")
		json.dump(element, open(path, "w+"))
	
class TinyScraper:
	def __init__(self, pool_size = 5, sleep_time = 1, fetcher = TinyFetcher, serializer = TinySerializer):
		self.threadLimit = pool_size
		self.sleepTime = sleep_time
		self.threadCounter = 0
		path = os.path.join(os.path.curdir, "contents")
		if not os.path.exists(path):
			os.mkdir(path)
		self.fetcher = fetcher()
		self.serializer = serializer()
		self.tasks = Queue()
		self.lock = threading.Lock()
		self.threads = {}

	def start(self):
		self.serializer.readIndex().recover(self.tasks.put)
		indexFetcher = threading.Thread(target = self.fetcher.fetchIndex, args = (self.serializer.appendIndex, self.tasks.put, ))
		indexFetcher.start()
		while True:
			while not self.tasks.empty():
				task = self.tasks.get()
				with self.lock:
					if (not task["finished"]) and (len(self.threads) < self.threadLimit):
						fetcher = threading.Thread(target = self.fetcher.fetchContent, args = (task, self.serializer.appendContent, self.release, ))
						fetcher.reboot = False
						fetcher.start()
						self.threads[fetcher] = 0
						print("[" + str(fetcher.name) + "] has started!!!")
			self.serializer.saveIndex()
			time.sleep(self.sleepTime)
			for thread in self.threads:
				self.threads[thread] += 1
				if self.threads[thread] > 180:
					thread.reboot = True

	def release(self, element):
		with self.lock:
			self.threads.pop(threading.current_thread())
			if threading.current_thread().reboot:
				self.tasks.put(element)
			print("[" + threading.current_thread().name + "] has stopped...")
		
class QuoraFetcher(TinyFetcher):
	def fetchIndex(self, appender, handler):
		wait_time = 7
		driver = self.load("https://www.quora.com/search?q=Shanghai")
		while True:
			links = BeautifulSoup(driver.page_source, "lxml").find_all("a", class_="question_link")
			for question in links:
				element = {"title" : question.contents[0].text, "link" : question["href"], "finished" : False}
				self.handleCallbacks(element, appender, handler)
			driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
			time.sleep(wait_time)
			
	def fetchContent(self, index, appender, handler):
		retry_limit = 3
		wait_time = 7
		print("Fetching question: " + index["title"])
		driver = self.load("https://www.quora.com/" + index["link"])
		soup = BeautifulSoup(driver.page_source, "lxml")
		try:
			ans_num_tot = int(soup.find("div", class_="answer_count").text.split(" ")[0].split("+")[0])
		except:
			ans_num_tot = 0
		ans_num_fetchable = 0
		attempt = {0 : 0}
		
		while ans_num_fetchable < ans_num_tot:
			if threading.current_thread().reboot:
				driver.quit()
				handler(index)
				return
			if attempt[ans_num_fetchable] > retry_limit:
				break
			soup = BeautifulSoup(driver.page_source, "lxml")
			answer_list = soup.find_all("div", class_="pagedlist_item")
			ans_num_fetchable = 0
			for ans in answer_list:
				if ans.find("a", class_="user"):
					ans_num_fetchable += 1
			try:
				driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
				time.sleep(wait_time)
			finally:
				if ans_num_fetchable in attempt:
					attempt[ans_num_fetchable] += 1
				else:
					attempt[ans_num_fetchable] = 1
					
		answer_list_origin = soup.find_all("div", class_="pagedlist_item")
		answer_list = []
		answer_num = 0
		for ans in answer_list_origin:
			if ans.find("a", class_="user"):
				answer_num += 1
				answer = {}
				answer["author"] = ans.find("a", class_="user").text
				if ans.find("span", class_="IdentityNameCredential NameCredential"):
					answer["credential"] = ans.find("span", class_="IdentityNameCredential NameCredential").text
				if ans.find("span", class_="datetime"):
					answer["date"] = ans.find("span", class_="datetime").text
				if ans.find("div", class_="ui_qtext_expanded"):
					answer["content"] = ans.find("div", class_="ui_qtext_expanded").text
				if ans.find("span", class_="meta_num"):
					answer["view"] = ans.find("span", class_="meta_num").text
				answer_list.append(answer)
		element = {}
		element["title"] = index["title"]
		element["link"] = index["link"]
		element["answers"] = answer_list
		element["finished"] = True
		driver.quit()
		self.handleCallbacks(element, appender, handler)