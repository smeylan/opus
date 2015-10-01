# programmatic interface for open_sub_2013

slowstoragedir = '/shared_hd0/corpora/OPUS/2013_OPUS'
languages = ['en'] #['en','cs','nl','fr','es','it','de']

directories = createDirectoryStructure('/shared_hd0/corpora/OPUS/2013_OPUS')


[downloadLanguage(x, directories['downloadpath']) for x in  languages]


[processLanguage(x, directories['downloadpath']) for x in  languages]


[combineLanguage(x, directories['downloadpath'], directories['combinedpath']) for x in  languages]

