import json
from openai import OpenAI
import os
import supabase
from time import time

print("Running db_bot.py!")

fdir = os.path.dirname(__file__)
def getPath(fname):
    return os.path.join(fdir, fname)

# Prevent committing config.json
# This is a local change. Reverse with:
#          git update-index --no-assume-unchanged config.json
os.system("git update-index --assume-unchanged '" + getPath("config.json") + "'")

# Read in setup scripts for context
setupSqlPath = getPath("setup.sql")
with (open(setupSqlPath) as setupSqlFile):
    setupSqlScript = setupSqlFile.read()

# Open SQL Connection & Cursor
sqlConnection, sqlCursor = supabase.openConnection()
def runSql(query):
    sqlCursor.execute(query)
    result = sqlCursor.fetchall()
    return result

# OPENAI
configPath = getPath("config.json")
print(configPath)
with open(configPath) as configFile:
    config = json.load(configFile)

openAiClient = OpenAI(api_key = config["openaiKey"])

def getChatGptResponse(content):
    stream = openAiClient.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": content}],
        stream=True,
    )

    responseList = []
    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            responseList.append(chunk.choices[0].delta.content)

    result = "".join(responseList)
    return result


# strategies
commonSqlOnlyRequest = " Give me a postgreSQL select statement that answers the question. Only respond with postgreSQL syntax. If there is an error do not explain it!"
strategies = {
    "zero_shot": setupSqlScript + commonSqlOnlyRequest,
    "single_domain_double_shot": (setupSqlScript +
                   " How many total employees of each job title are scheduled this week? " +
                   "\nSELECT COALESCE(SUM(t.num_employees), 0) AS total_employees\nFROM public.task t\nWHERE t.start_date >= CURRENT_DATE \n    AND t.start_date < CURRENT_DATE + INTERVAL '7 days';" +
                   commonSqlOnlyRequest)
}

questions = [
    # "I need to create two-three groups into my table. Can you please generate the SQL to create a group based in Antarctica and another group based in Puerto Rico?",
    "Which roles in the company are the most expensive?",
    "What is the percentage complete of each open project? (What percentage of it's tasks are at 100 percent complete?)",
    "What is the total square footage covered by each project?",
    "How many total employees of each job title are scheduled this week?",
    "How many total employees of each job title are scheduled each week (this week, next week, ... up to 6 weeks in the future)?",
    "How many tasks do not have an rp_request_id?",
    "Which groups have the most active projects?",
    "How much am I spending today on all my tasks and projects?",
    "How many total employees are required to be working on all the tasks?",
    "What is the maximum number of employees that will be required on a weekly basis in the next year?",
    "What is the minimum number of employees that will be required on a weekly basis in the next year?",
]

def sanitizeForJustSql(value):
    gptStartSqlMarker = "```sql"
    gptEndSqlMarker = "```"
    if gptStartSqlMarker in value:
        value = value.split(gptStartSqlMarker)[1]
    if gptEndSqlMarker in value:
        value = value.split(gptEndSqlMarker)[0]

    return value

for strategy in strategies:
    responses = {"strategy": strategy, "prompt_prefix": strategies[strategy]}
    questionResults = []
    for question in questions:
        print(question)
        error = "None"
        try:
            sqlSyntaxResponse = getChatGptResponse(strategies[strategy] + " " + question)
            sqlSyntaxResponse = sanitizeForJustSql(sqlSyntaxResponse)
            print(sqlSyntaxResponse)
            queryRawResponse = str(runSql(sqlSyntaxResponse))
            print(queryRawResponse)
            friendlyResultsPrompt = "Here is the PostgreSQL schema that was used as context:"
            friendlyResultsPrompt += setupSqlScript
            friendlyResultsPrompt += "\n\nI asked a question \"" + question +"\" and the response was \""+queryRawResponse+"\"."
            friendlyResultsPrompt += "\n\nPlease, just give a concise response in a more friendly way? Please do not give any other suggests or chatter."
            friendlyResponse = getChatGptResponse(friendlyResultsPrompt)
            print(friendlyResponse)
        except Exception as err:
            error = str(err)
            print(err)

        questionResults.append({
            "question": question,
            "sql": sqlSyntaxResponse,
            "queryRawResponse": queryRawResponse,
            "friendlyResponse": friendlyResponse,
            "error": error
        })

    responses["questionResults"] = questionResults

    with open(getPath(f"response_{strategy}_{time()}.json"), "w") as outFile:
        json.dump(responses, outFile, indent = 2)


sqlCursor.close()
sqlConnection.close()
print("Done!")
