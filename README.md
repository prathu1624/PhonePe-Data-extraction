# PhonePe-Data-extraction
In this project, I have used PhonePe's GitHub repository, which contains a wealth of transaction and user device data from 2018 to 2022, for this research. Condensing this data into an instructive dashboard was the main objective. For this work, PostgreSQL was used for data storage, Python was used for scripting, and Streamlit was used to create an interactive user interface.
JSON data is extracted and then converted into a structured data frame to start the procedure. This data is then arranged and placed in a PostgreSQL database. The last step is retrieving the data and dynamically displaying it on a dashboard with the Streamlit framework.
Execution of the process is simple on a Windows PC. To launch the dashboard, just open Command Prompt (CMD), locate the directory containing the Python code (with a.py extension), and type the command 'streamlit run file.py'.
The Streamlit dashboard is thoughtfully structured into four categories:

Transaction Overview (State and District Wise): Utilizing choropleth maps to depict transaction trends based on geographical locations.

User Analysis: Offering insights into user behavior, preferences, and patterns.

Payment Type Analysis: Providing a detailed breakdown of transaction types through pie charts.

User Device Analysis: Presenting data on devices used by users through bar charts.

This streamlined and insightful dashboard is a comprehensive tool for analyzing transactional and user data, enhancing decision-making processes based on the information gleaned from PhonePe's repository.


