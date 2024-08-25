library(stringr)


# Read the content of the .qmd file
file_content <- readLines("./chapters for the book/03-Architecture.qmd")

# Combine lines into a single text string
text <- paste(file_content, collapse = " ")

# Count words using stringr
word_count_03 <- str_count(text, "\\w+")

# Print the word count

word_count_01 +
word_count_02 +
word_count_03 +
word_count_04
