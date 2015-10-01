#!/usr/bin/python
# -*- coding: utf-8 -*-

import urllib2, gzip, os, glob, re, codecs, tarfile, shutil, multiprocessing, pdb, string, itertools, numpy, pandas
from xml.dom.minidom import parse, parseString
import urllib, time

def makeDirectoryStructure(slowstoragedir):
	'''build the directory structure for holding the downloads and processed files'''
	downloadpath = os.path.join(slowstoragedir,'downloaded')	
	if not os.path.exists(downloadpath):
		os.makedirs(downloadpath)
	expandpath = os.path.join(slowstoragedir,'expanded')	
	if not os.path.exists(expandpath):
		os.makedirs(expandpath)	
	combinedpath = os.path.join(slowstoragedir,'combined')
	if not os.path.exists(combinedpath):
		os.makedirs(combinedpath)
	return({'downloadpath': downloadpath, 'combinedpath':combinedpath, 'expandpath':expandpath})


def downloadLanguage(language, downloadpath, expandpath):
	'''download the tar file and extract it for a specific language'''		
	years = ['2012', '2013']	
	filename = language+'.tar.gz'			   
	for year in years:
		if not os.path.exists(os.path.join(downloadpath, year + '_' + filename)):
			url = 'http://opus.lingfil.uu.se/download.php?f=OpenSubtitles'+year+'/'+filename
			urllib.urlretrieve(url, os.path.join(downloadpath,year+'_'+filename))	
		tar = tarfile.open(os.path.join(downloadpath,year+'_'+filename), 'r')
		success = [tar.extract(item, expandpath) for item in tar]	

		#missing XML node in 2012 data-- !!! not sure the programmatic fix works
		os.system('mv '+os.path.join(expandpath, 'OpenSubtitles2012',language)+' '+os.path.join(expandpath, 'OpenSubtitles2012','xml'))
	print('Finished dowloading and extracting language: '+language)

def processLanguage(language, expandpath, outputdir):
	if not os.path.exists(os.path.join(outputdir, language)):
		os.makedirs(os.path.join(outputdir, language))
	
	#parallel treatment
	start_time =  time.time()

	# Put the manager in charge of how the processes access the list
	mgr = multiprocessing.Manager()
	myList = mgr.list() 
    
	# FIFO Queue for multiprocessing
	q = multiprocessing.Queue()
    
	# Start and keep track of processes
	procs = []
	for i in range(24):
		p = extractionWorker( q,myList )
		procs.append(p)
		p.start()
		
	subPaths = [x[0] for x in os.walk(os.path.join(expandpath, 'OpenSubtitles2013', 'xml' ,language))]
	files = []
	[files.append(glob.glob(path+'/*.xml.gz')) for path in subPaths]	
	files = [x[0] for x in files if len(x) > 0]

	if len(files) == 0:			
		raise ValueError('No files found')		
	         	
	filesizes = [(x, os.stat(x).st_size) for x in files]
	filesizes.sort(key=lambda tup: tup[1], reverse=True) #start with the biggest files
	
	# Add data, in the form of a dictionary to the queue for our processeses to grab    
	extension = '.txt'
	[q.put({"inputfile": file[0], "outputfile": os.path.join(outputdir, language, os.path.splitext(os.path.basename(file[0]))[0]+extension)}) for file in filesizes] 
      
	#append none to kill the workers with poison pills		
	for i in range(24):
		q.put(None) 
        
	# Ensure all processes have finished and will be terminated by the OS
	for p in procs:
		p.join()     
        
	for item in myList:
		print(item)

	print('Done! processed '+str(len(myList))+' files; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes') 	


class extractionWorker(multiprocessing.Process):
    '''single-thread worker for parallelized decompression, parsing, and cleaning of XML files'''  
    def __init__(self,queue,myList):
        super(extractionWorker, self).__init__()
        self.queue = queue
        self.myList = myList
        
    def run(self):    	
        for job in iter(self.queue.get, None): # Call until the sentinel None is returned
        	try:
        		extractText(job['inputfile'], job['outputfile'])        
        	except ValueError:
        		print 'Problems encountered in cleaning '+job['inputfile']
			self.myList.append(job['inputfile'])


def extractText(inputfile, outputfile, remap=None):	
	'''unzip, parse the XML, and extract the sentences from a compressed movie file ffom OPUS''' 
	if not remap: #if not specified, get the default word replacement hash table
		remap = makeRemap()

	#!!! are the libraries globally accessible?
	#characters to clean out	
	exclude = set(string.punctuation)
	exclude.update(' ')
	exclude.remove("'")

	dom = parseString(gzip.open(inputfile).read())
	sentence_list = dom.getElementsByTagName('s')
	parsed_sentences = []
	for s in sentence_list:
		words = [w.firstChild.nodeValue for w in s.getElementsByTagName('w') if w.firstChild != None]
		words = [''.join(ch for ch in s if ch not in exclude) for s in words]
		sentence = " ".join([w for w in words if w != ""])
		sentence = re.sub(" '","'", sentence, count= 0)		
		parsed_sentences.append(' '.join([wordProcess(x, remap) for x in sentence.split(' ')]))
	movie_string = '\n'.join(parsed_sentences) #all text in the movie	
	outfile = codecs.open(outputfile, 'w', 'utf-8')
	outfile.write(movie_string)
	print('added unigrams from '+inputfile)
	outfile.close()

def wordProcess(x, remap):
	#map some tokens onto their replacements; if not found, return the word
	if x in remap.keys():
		return(remap[x])
	else:
		return(x)	

def expandgrid(*itrs):
   product = list(itertools.product(*itrs))
   return {'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))}

def makeRemap():
	#build a dictionary for replacing contractions with their longforms, uppercase closed-class words with lowercase ones
	pronominals = ['I','We', 'You', 'He', 'She', 'They', 'It', 'This', 'There', 'That', 'Here','Where', 'How', 'Who', 'What', 'When', 'Why']

	shortForms = ["'ve", "'m", "'s", "'re", "'ll"]
	longForms = ['have', 'am','is', 'are', "will"]
	verbs = ['Was','Are','Can','Must','Should','Could', 'Do', 'Is', 'Would', 'Did']

	replaceGrid = pandas.DataFrame(expandgrid(pronominals, shortForms)) #unattested forms are fine-- they just don't show up in the corpus
	replaceGrid.columns = ['pronouns','aux']

	replaceGrid['searchString'] =  replaceGrid['pronouns'].map(str) + replaceGrid['aux'].map(str)

	d = {'shortForms' : shortForms ,'longForms' : longForms}
	
	replaceGrid = pandas.merge(replaceGrid, pandas.DataFrame(d), left_on='aux', right_on='shortForms')
	replaceGrid['replaceString'] =  replaceGrid['pronouns'].map(str) + ' ' + replaceGrid['longForms'].map(str)
	rg = replaceGrid.loc[:,('searchString','replaceString')]
	rg['replaceString'] = [x.lower() for x in rg['replaceString']]
	rg.lc = rg.copy()
	rg.lc['searchString'] = [x.lower() for x in rg.lc['searchString']]


	negGrid = pandas.DataFrame({'verbs' : verbs, 'shortForms': "n't", 'longForms' :'not'})
	
	negGrid['searchString'] = negGrid['verbs'].map(str) + negGrid['shortForms'].map(str)
	negGrid['replaceString'] = negGrid['verbs'] + ' ' + negGrid['longForms']
	
	ng = negGrid.loc[:,('searchString','replaceString')]
	ng['replaceString'] = [x.lower() for x in ng['replaceString']]
	ng.lc = ng.copy()
	ng.lc['searchString'] = [x.lower() for x in ng.lc['searchString']]

	functionWordList = ['about','across','against','along ','around','at','behind','beside','besides','by','despite','down','during','for','from','in','inside','into','near','of','off','on','onto','over','through','to','toward','with','within','without','you','he','she','me','her','him','my','mine','her','hers','his','myself','himself','herself','anything','everything','anyone','everyone','ones','such','it','we','they','us','them','our','ours','their','theirs','itself','ourselves','themselves','something','nothing','someone','the','some','this','that','every','all','both','one','first','other','next','many','much','more','most','several','no','a','an','any','each','no','half','twice','two','second','another','last','few','little','less','least','own','and','but','after','when','as','because','if','what','where','which','how','than','or','so','before','since','while','although','though','who','whose','can','may','will','shall','could','might','would','should','must','be','do','have','here','there','today ','tomorrow ','now','then','always ','never','sometimes','usually ','often','therefore','however','besides','moreover','though','otherwise','else','instead','anyway','incidentally','meanwhile','i','was', 'uh','um','um-hum','um-huh','uh-huh', "uh-hum",'is','are','oh','yeah','not','yes','okay','yep','were',"i-",'hi','hello','had', 'got','been', 'has', 'those','does','huh']
	fnw = pandas.DataFrame({'searchString':[x.capitalize() for x in functionWordList], 'replaceString':functionWordList})	

	combined = pandas.concat([rg, rg.lc, ng, ng.lc, 
		fnw])
	rdict = dict(zip(combined['searchString'], combined['replaceString']))
	#some irregulars
	rdict["Can't"] = "can not"
	rdict["can't"] = "can not"
	rdict["Musn't"] = "must not"
	rdict["musn't"] = "must not"
	return(rdict)

def combineLanguage(combinedpath, outputfile):
	''' wrapper for cat'''
	os.system('find '+combinedpath+" -name '*.txt' -exec cat {} + > "+outputfile)
	print('Text files combined')

