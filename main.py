# programmatic interface for open_sub_2013

slowstoragedir = '/shared_hd0/corpora/OPUS/2013_OPUS'
languages = ['en','es','it','de'] 
numItems = 25000
import opus, pdb, os, ngrok



directories = opus.makeDirectoryStructure('/shared_hd0/corpora/OPUS/2013_OPUS')
#[opus.downloadLanguage(x, directories['downloadpath'], directories['expandpath']) for x in  languages]
#[opus.processLanguage(x, directories['expandpath'], directories['combinedpath']) for x in  languages]
[opus.combineLanguage(os.path.join(directories['combinedpath'], x), os.path.join(directories['intermediatecountpath'],x+'_combined.txt')) for x in  languages]


for language in languages:
	print('Counting ngrams...')
	countfile = os.path.join(directories['intermediatecountpath'],language+'_counted.txt')
	ngrok.countNgrams(os.path.join(directories['intermediatecountpath'],language+'_combined.txt'), countfile, n=1)

	print('Rearranging ngrams...')
	rearrangedFile = os.path.join(directories['intermediatecountpath'],language+'_2013_rearrange_counts.txt')
	ngrok.rearrangeNgramFile(countfile, rearrangedFile , reverse=False)

	print('Sorting ngrams...')
	sortedfile =  os.path.join(directories['intermediatecountpath'],language+'_2013_sorted.txt')	
	ngrok.sortNgramFile(rearrangedFile,  sortedfile)

	print('Marginalizing ngrams...')
	marginalizedfile = os.path.join(directories['intermediatecountpath'],language+'_2013_unigram_list.txt')
	ngrok.marginalizeNgramFile(sortedfile,  marginalizedfile, n=1, sorttype='numeric')

	print('Cleaning ngrams...')
	outputfile = os.path.join(slowstoragedir, language+'_2013.txt')
	ngrok.cleanUnigramCountFile(marginalizedfile, outputfile, numItems, language)	


#these intermediate files should go into a directory	