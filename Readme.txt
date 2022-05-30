########################################################## PREREQUISITES
- Create a Python environment. Ensure pip and python 3.8 are installed.

########################################################## DOWNLOADING REQUIREMENTS FILE; SETTING API-KEY LOCATION; RUNNING PART 1 OF SUBMISSION 
- Note: this will save to the current working directory. 
- Modify the location of the API key at the position below named <fileLocation.json> (No apostrophes around the <fileLocation.json>). Once this is done, run the command below to test part 1 of the submission.
+ pip install -r requirements.txt & python.exe submissionPart1Final.py --input <fileLocation.json>

########################################################## 
- After having run the command above, the required packages for part 2 of the submission and unittesting should be downloaded. 

########################################################## RUNNING PART 2 OF SUBMISSION 
- Now you can run the command below to test part 2 of the submission. Make sure to set the location of the API key to <fileLocation.json> (with no apostrphes around the fileLocation).
+ set GOOGLE_APPLICATION_CREDENTIALS=<fileLocation.json> & python.exe submissionPart2Final.py

########################################################## RUNNING THE UNITTEST
- To run the unit test on the composite-transformation using Apache Beam, please run the below command:
+ python -m unittest2 submissionPart2Final_UnitTesting.py