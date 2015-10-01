# programmatic interface for open_sub_2013

slowstoragedir = '/shared_hd0/corpora/OPUS/2013_OPUS'
languages = ['de','it'] 
import opus, pdb, os

directories = opus.makeDirectoryStructure('/shared_hd0/corpora/OPUS/2013_OPUS')
[opus.downloadLanguage(x, directories['downloadpath'], directories['expandpath']) for x in  languages]
[opus.processLanguage(x, directories['expandpath'], directories['combinedpath']) for x in  languages]
[opus.combineLanguage(os.path.join(directories['combinedpath'], x), os.path.join(slowstoragedir, x+'_2013.txt')) for x in  languages]

