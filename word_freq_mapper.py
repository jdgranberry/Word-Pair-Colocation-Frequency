#!/usr/bin/python

''' This script is a mapper function for use with Hadoop that reads input from 
    stdin and count that pairs of words appear within a distance in words 
    (specified by MAX_DIST) together. 
    
    i.e. In the following sentence,
    "The boy walks the dog. The boy gives a treat to the dog."
    we get the following lines of output (among others):
        the boy 0   2
        boy dog 2 1
        boy dog 5 1
        
    or  pre suc n m
   
    'pre' is followed by 'suc' after 'n' intermediate words 'm' times within the
    input.
    
    This was created primarily as a demonstration of the ability to combine
    Python scripting with Hadoop's MapReduce function using the Streaming API
    to read and write with stdin and stdout.
'''
import sys
import string

MAX_DIST = 10 # Max distance between words to consider

def main():

    # initialize the main array
    main_words_list = []
    word_count = init_array(main_words_list, MAX_DIST + 2)
    
    # continue to process stdin
    unprocessed_words = []
    for line in sys.stdin:
        words = clean_and_split_line(line)
        for word in words:
            word_count = process_new_word(word, main_words_list, word_count)
           
def init_array(main_words_list, max_length):
    ''' Initialize the distance array by filling with words from the buffer, then
    seeking past the read lines.'''
    
    # Fill the array by reading lines from stdin and appending to main list 
    # until the main list is filled. The array is a list of tuples, where each
    # tuple is the word read into it and the current word count.
    array_filled = False
    array_length = MAX_DIST + 2
    word_count = 0
    words = []
    for line in sys.stdin:
        words += clean_and_split_line(line)
        if (not array_filled):
            for word in words:
                if (not array_filled):
                    word_count += 1
                    main_words_list.append((words.pop(0), word_count))
                    array_filled = (len(main_words_list) == array_length) 
              
        else: # (array_filled)
            break  
        
    # Process the initialized list of tuples.
    for i in range(0, len(main_words_list)):
        for j in range(0, i):
            print_output(main_words_list[j][0],
                         main_words_list[i][1] - main_words_list[j][1] - 1, 
                         main_words_list[i][0])
    
    # Process remaining words read from stdin but not added to main list
    for word in words:
        word_count = process_new_word(word, main_words_list, word_count)
        
    return word_count
    

def process_new_word(new_word, words_list, word_count):
    ''' Add a new word to the list, then count the distance from the previous
    words to the new word and write to output.  '''
    word_count += 1
    curr = word_count % len(words_list)
    words_list[curr] = (new_word, word_count)
    count_down = MAX_DIST
    for i in range(curr + 1, curr + len(words_list)):
            print_output(words_list[i % len(words_list)][0],
                         count_down,
                         words_list[curr][0])
            count_down -= 1
    return word_count

def clean_and_split_line(line):
    ''' Remove punctuation, leading/trailing whitespace, capitalization from 
    line. 
    Return line as a list of words. '''
    # remove leading and trailing whitespace and convert to lower case
    line = line.strip().lower()
    # remove punctuation
    line = line.translate(string.maketrans("",""), string.punctuation)
    # split the line into a list of words
    words = line.split()
    return words

def print_output(predecessor, distance_from, successor):
    ''' Print output.
        Primary 3-tuple is seperated by spaces then followed by a tab and a 
        trivial count of 1 during mapper phase. This will be input for the 
        reducer.'''
    print '%s %s %s\t%s' % (predecessor, successor, distance_from, 1)
    
main()
        
        
        
    

    
   
    
    
