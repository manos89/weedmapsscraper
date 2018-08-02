# -*- coding: utf-8 -*-
try:
	from jscraper import *
	import json,requests,sqlite3,re
	from time import gmtime, strftime
	import time
except Exception as E:
	print E
	time.sleep(60)
DictList=[]
count=0
DisCount=0
Today=strftime("%B%d%Y", gmtime())



def get_dispensaries(queue):
	global DisCount,DisCount
	queue_full = True
	while queue_full:
		try:
			Link= queue.get(False)
			try:
				NoJson=requests.get(Link).text
				d=json.loads(NoJson)
				DispeDict={}
				DispeDict['id']=d['id']
				DispeDict['name']=d['name']
				DispeDict['type']=d['_type']
				DispeDict['latitude']=d['latitude']
				DispeDict['longitude']=d['longitude']
				DispeDict['address']=d['address']
				DispeDict['zip_code']=d['zip_code']
				DispeDict['city']=d['city']
				DispeDict['email']=d['email']
				DispeDict['state']=d['state']
				DispeDict['web_url']=d['url']
				DispeDict['ranking']=d['ranking']
				DispeDict['rating']=d['rating']
				DispeDict['reviews_count']=d['reviews_count']
				DispeDict['license_type']=d['license_type']
				DispeDict['InWeedmaps']=d['published']
				DictList.append(DispeDict)
				print DisCount
				DisCount+=1
			except Exception as E:
				pass
			q2.task_done()
		except Queue.Empty:
			queue_full = False	


def get_possible_links():
	Links=[]
	Base_URL='https://weedmaps.com/api/web/v1/listings/'
	for i in range(1,60000):
		Links.append(Base_URL+str(i))
	return Links


def cleanhtml(raw_html):
	cleanr = re.compile('<.*?>')
	cleantext = re.sub(cleanr, '', raw_html)
	return cleantext

def write_database(db_file,infolist):
	try:
		conn = sqlite3.connect(db_file)
		c = conn.cursor()
		try:
			c.execute('''CREATE TABLE '''+Today+''' (SHOPID text, NAME text, TYPE text,EMAIL text,PHONE text,LAT text,LONG text,ADDRESS text, ZIPCODE text, CITY text, STATE text, WEEDURL text, RANKING text, RATING text, NOREVIEWS text,TOTALHITS text ,LICENSETYPE text,SUNDAY text,MONDAY text, TUESDAY text,WEDNESDAY text, THURSDAY text, FRIDAY text, SATURDAY text, WEBSITE text, TWITTER text, FACEBOOK text, INSTAGRAM text, MEMBERSINCE text,PUBLIC text)''')
		except:
			pass
		c.executemany('INSERT INTO '+Today+' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',[infolist])
		conn.commit()
		conn.close()
	except Exception as e:
		print e


def unique_check(db_file,shopid):
	try:
		conn = sqlite3.connect(db_file)
		c = conn.cursor()
		try:
			c.execute("SELECT * FROM "+Today+" WHERE SHOPID = ?", (shopid,))
			data=c.fetchall()
			if len(data)==0:
				return True
			else:
				return False
		except Exception as E:
			pass
	except Exception as E:
		pass
	return True


def get_dictionary():
	global DisCount
	# url='https://api-v2.weedmaps.com/api/v2/listings?&filter[plural_types][0]=doctors&filter[plural_types][1]=dispensaries&filter[plural_types][2]=deliveries&page_size=150&size=10000000&page={PN}'
	url1='https://api-v2.weedmaps.com/api/v2/listings?&filter[plural_types][0]=deliveries&page_size=150&size=10000000&page={PN}'
	url2='https://api-v2.weedmaps.com/api/v2/listings?&filter[plural_types][0]=doctors&page_size=150&size=10000000&page={PN}'
	url3='https://api-v2.weedmaps.com/api/v2/listings?&filter[plural_types][0]=dispensaries&page_size=150&size=10000000&page={PN}'
	urls=[url1,url2,url3]	
	urls=[url1]
	DictList=[]
	for url in urls:
		i=1
		while True:
			try:
				r=requests.get(url.replace('{PN}',str(i))).text
				data=json.loads(r)
				for d in data['data']['listings']:
					DispeDict={}
					DispeDict['id']=d['id']
					DispeDict['name']=d['name']
					DispeDict['type']=d['type']
					DispeDict['latitude']=d['latitude']
					DispeDict['longitude']=d['longitude']
					DispeDict['address']=d['address']
					DispeDict['zip_code']=d['zip_code']
					DispeDict['city']=d['city']
					DispeDict['state']=d['state']
					DispeDict['web_url']=d['web_url']
					DispeDict['ranking']=d['ranking']
					DispeDict['rating']=d['rating']
					DispeDict['reviews_count']=d['reviews_count']
					DispeDict['license_type']=d['license_type']
					DispeDict['InWeedmaps']='true'
					DictList.append(DispeDict)
				print DisCount
				DisCount+=1
				i+=1
			except Exception as E:
				print E
				break
	return DictList

def get_hits(soup):
	Hits=jscraper.get_text_element(soup=soup,TagName='div',AttributeName='class',AttributeValue='listing-hits-number')[0]
	return Hits


def get_days(soup):
	Days={}
	DaysPlace=jscraper.get_classes(soup=soup,TagName='div',AttributeName='class',AttributeValue='details-card-items')[0]
	for Day in DaysPlace:
		D=jscraper.get_text_element(soup=Day,TagName='div',AttributeName='class',AttributeValue='hours-item-label')[0]
		Hour=jscraper.get_text_element(soup=Day,TagName='div',AttributeName='class',AttributeValue='hours-item-data')[0]
		Hour=cleanhtml(Hour)
		Hour=Hour.replace('\xc2\xa0-\xc2\xa0  ','-').encode('utf-8','replace')
		Days[D]=Hour.decode('utf8')
	return Days

def get_social(soup):
	Social={}
	SocialPlace=jscraper.get_classes(soup=soup,TagName='div',AttributeName='class',AttributeValue='social-links')[0]
	for S in SocialPlace:
		SocialName=jscraper.get_text_element(soup=S,TagName='div',AttributeName='class',AttributeValue='details-card-item-label')[0]
		try:
			SocialLink=jscraper.get_links(soup=S)[0]
		except:
			SocialLink=''
		Social[SocialName]=SocialLink.decode('utf8')
	return Social

def get_member_since(soup):
	MemberSince=jscraper.get_text_element(soup=soup,TagName='div',AttributeName='class',AttributeValue='details-card-item-data')[-1]
	return MemberSince.decode('utf8')

def get_info(queue):
	global count
	queue_full = True
	while queue_full:
		try:
			DispeDict= queue.get(False)
			try:
				Unique_check=unique_check('final43.db',DispeDict['id'])
				if Unique_check==True:
					soup=jscraper.get_soup(url=DispeDict['web_url']+'#/details')
					try:
						Email=jscraper.get_text_element(soup=soup,TagName='a',AttributeName='itemprop',AttributeValue='email')[0]
						DispeDict['email']=Email.decode('utf-8')
					except:
						pass
					try:
						Phone=jscraper.get_text_element(soup=soup,TagName='a',AttributeName='itemprop',AttributeValue='telephone')[0]
						DispeDict['phone']=Phone.decode('utf-8')
					except:
						DispeDict['phone']=''
					try:
						Days=get_days(soup)
					except:
						Days={}
						Days['Sunday']=Days['Monday']=Days['Tuesday']=Days['Wednesday']=Days['Thursday']=Days['Friday']=Days['Saturday']=''
					try:
						Social=get_social(soup)
					except:
						Social={'Website':'','Twitter':'','Facebook':'','Instagram':''}
					try:
						DispeDict['MemberSince']=get_member_since(soup)
					except:
						DispeDict['MemberSince']=''
					try:
						DispeDict['TotalHits']=get_hits(soup)
					except:
						DispeDict['TotalHits']=''
					if DispeDict['InWeedmaps']==1:
						DispeDict['InWeedmaps']='true'
					elif DispeDict['InWeedmaps']==0:
						DispeDict['InWeedmaps']='false'
					writelist=[DispeDict['id'],DispeDict['name'],DispeDict['type'],DispeDict['email'],DispeDict['phone'],DispeDict['latitude'],DispeDict['longitude'],DispeDict['address'],DispeDict['zip_code'],DispeDict['city'],
					DispeDict['state'],DispeDict['web_url'],DispeDict['ranking'],DispeDict['rating'],DispeDict['reviews_count'],DispeDict['TotalHits'],DispeDict['license_type'],Days['Sunday'],Days['Monday'],Days['Tuesday'],Days['Wednesday'],Days['Thursday'],Days['Friday'],Days['Saturday'],Social['Website'],Social['Twitter'].decode('utf-8'),Social['Facebook'].decode('utf-8'),Social['Instagram'].decode('utf-8'),DispeDict['MemberSince'],DispeDict['InWeedmaps']]
					write_database('final43.db',writelist)
			except Exception as E:
				print E
				pass
			print 'DONE',count
			count+=1
			q.task_done()
		except Queue.Empty:
			queue_full = False



print 'Collecting links from weedmaps'
Links=get_possible_links()
q2 = Queue.Queue()

for link in Links:
	q2.put(link)



thread_count = 12


for i in range(thread_count):
	t = threading.Thread(target=get_dispensaries, args = (q2,))
	t.start()

q2.join()
print len(DictList)



DictList+=get_dictionary()

DictList=[dict(t) for t in set([tuple(d.items()) for d in DictList])]
print 'DONE', len(DictList)


print 'Will now start collecting information for each entry'
q = Queue.Queue()

for link in DictList:
	q.put(link)




print 'Collecting information from each link'
thread_count = 12


for i in range(thread_count):
	t = threading.Thread(target=get_info, args = (q,))
	t.start()

q.join()

print 'FINISHED'