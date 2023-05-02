# PhonePe-Data-extraction
Hi! guys, this is my second project in GUVI Data Science Course .
In this project I have used the github repository for phonepe which contains transaction and user device data from 2018-2022.
The task was to create a dashboard by extracting this data.
Tools used are Python, Postgresql and streamlit.
The basic operation is to extract the data which is in JSON format and convert it to a dataframe.
This extracted data is then moved to SQL (postgresql) and then fetched to display in the form of a dashboard using streamlit library.
This program was written in a windows system, so the basic execution is as follows:
start CMD and enter the path of the python file with extension (.py).
Then type the command "streamlit run 'file.py'(name of python file)".
The dashboard in streamlit has four categories : transaction(state and district wise), User analysis, payment type analysis and uder device analysis.
In category 1 and 2 choropleth maps are used to display the data at their respective longitudes and latitudes.
Tabs 3 and 4 have pie and bar chart to analyze data based on transaction types and devices used by users.
