# programmatic interface for open_sub_2013

slowstoragedir = '/shared_hd0/corpora/OPUS/2013_OPUS'
languages = ['ru'] #'cs','pl','ro','sv'
numItems = 25000
import opus, pdb, os, ngrok



directories = opus.makeDirectoryStructure('/shared_hd0/corpora/OPUS/2013_OPUS')
[opus.downloadLanguage(x, directories['downloadpath'], directories['expandpath']) for x in  languages]

[opus.processLanguage(x, directories['expandpath'], directories['combinedpath']) for x in  languages]
[opus.combineLanguage(os.path.join(directories['combinedpath'], x), os.path.join(directories['intermediatecountpath'],x+'_combined.txt')) for x in  languages]


for language in languages:
	print('Counting ngrams...')
	countfile = os.path.join(directories['intermediatecountpath'],language+'_counted.txt')
	ngrok.countNgrams(os.path.join(directories['intermediatecountpath'],language+'_combined.txt'), countfile, n=1)

	print('Rearranging ngrams...')
	rearrangedFile = os.path.join(directories['intermediatecountpath'],language+'_2013_rearrange_counts.txt')
	ngrok.rearrangeNgramFile(countfile, rearrangedFile , reverse=False)

	print('Cleaning ngrams...')
	cleanedFile =  os.path.join(directories['intermediatecountpath'],language+'_2013_cleaned.txt')	
	ngrok.cleanUnigramCountFile(rearrangedFile, cleanedFile, numItems, language)	

	print('Sorting ngrams...')
	sortedfile =  os.path.join(directories['intermediatecountpath'],language+'_2013_sorted.txt')	
	ngrok.sortNgramFile(cleanedFile,  sortedfile)

	print('Collapsing ngrams...')
	collapsedfile =  os.path.join(directories['intermediatecountpath'],language+'_2013_collapsed.txt')	
	ngrok.collapseNgrams(sortedfile, collapsedfile)


	print('Marginalizing ngrams, first pass...')	
	ngrok.marginalizeNgramFile(collapsedfile,  os.path.join(slowstoragedir,language+'_2013.txt'), n=1, sorttype='numeric')

	


#!!!num items is deprecated
#moved the cleaning step up so that we don't produce equivalent items at the end. May increase the number of calls to Aspell