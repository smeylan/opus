#!/usr/bin/python
# -*- coding: utf-8 -*-

#this file provides CLI hooks for the functions in the OPUS library. For running many kabgyages, look at main.py
import opus, click

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(CONTEXT_SETTINGS)
def cli():
    pass

#makeDirectoryStructure
@cli.command()
@click.option('--slowstoragedir', type=click.Path(), help="Directory on the largest storage medium available", required=True)
def makeDirectoryStructure(slowstoragedir):
	'''build the directory structure for holding the downloads and processed files'''
	opus.makeDirectoryStructure(slowstoragedir)

#downloadLanguage
@cli.command()
@click.option('--language', type=str, help="2-letter designation for the language", required=True)
@click.option('--downloadpath', type=click.Path(), help="Directory where the downloaded, archived files should go", required=True)
@click.option('--expandpath', type=click.Path(), help="Directory where the download should be expanded to", required=True)
def downloadLanguage(language, downloadpath, expandpath):
	'''download the tar file and extract it for a specific language'''
	opus.downloadLanguage(language, downloadpath, expandpath)

#extractText
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Path of the compressed input file", required=True)
@click.option('--outputfile', type=click.Path(), help="Path for the yielded text file", required=True)
def extractText(inputfile, outputfile):
	'''unzip, parse the XML, and extract the sentences from a compressed movie file ffom OPUS''' 
	opus.extractText(inputfile, outputfile)	

#processLanguage
@cli.command()
@click.option('--language', type=str, help="2-letter designation for the language", required=True)
@click.option('--expandpath', type=click.Path(), help="Directory of the expanded corpus", required=True)
@click.option('--outputdir', type=click.Path(), help="Directory where processed text files should go", required=True)
def processLanguage(language, expandpath, outputdir):
	'''Run extract text on a large number of .gz files'''
	opus.processLanguage(language, expandpath, outputdir)




@cli.command()
@click.option('--combinedpath', type=click.Path(), help="Directory of the cleaned, individual text files", required=True)
@click.option('--outputfile', type=click.Path(), help="Path of the combined file", required=True)	
def combineLanguage(combinedpath, outputfile):
	opus.combineLanguage(combinedpath, outputfile)	
	
if __name__ == '__main__':
	cli()