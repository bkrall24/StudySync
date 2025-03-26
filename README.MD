# StudySync

StudySync is a custom set of modules for scraping data from word documents, organizing key data into relational databases, and associated functions to access, search, and update databases. 

## Usage

#### custom_database
Uses polars to access databases stored in csv files in a set folder location. Designed to create straightforward functions to perform necessary CRUD functions while following rules and schema for databases. Used as the base for end-user applications  

#### nlp_funcs
Some useful functions for matching specific words

#### study_scraping
Generates an object MeliorStudy using a word document. Scrapes key information from the word doc and filename using pythondocx. Includes a custom class built from the custom_databases classes to create a database from scratch from a given directory containing word docs. 

#### proposal_filling
Fills a proposal template

#### comp_searcher
Custom application in Tkinter to provide end-users the ability to search databases on specific criteria. Links directly to matching documents

#### proposal_generator
Custom application in Tkinter to generate a proposal using standard reference data.

## License
MIT License

Copyright (c) 2025 Rebecca Krall

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
